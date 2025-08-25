import "dotenv/config";
import { initKafka, getConsumer, shutdownKafka } from "./kafka";
import { env } from "./env";
import { upsertTick, flushDue, getBucketStats } from "./aggregator";
import { createHealthServer } from "./health";
import type { EachMessagePayload } from "kafkajs";
import type { PriceTick } from "./types";
import { printBanner } from "@cryptoflow/banner";
import logger from "./logger";

let running = true;
const state = {
  lastTickIso: null as string | null,
  lastFlushIso: null as string | null,
};

let totalTicksReceived = 0;
let totalTicksProcessed = 0;
let totalTicksSkipped = 0;
let totalFlushes = 0;
let totalWindowsFlushed = 0;
const symbolStats = new Map<
  string,
  { received: number; processed: number; lastPrice: number }
>();

async function handleMessage(payload: EachMessagePayload) {
  const { message, partition, topic } = payload;

  if (!message.value) {
    logger.warn("Received message with no value", { partition, topic });
    return;
  }

  try {
    totalTicksReceived++;

    const tick = JSON.parse(message.value.toString()) as PriceTick;
    const symbol = (tick.baseAsset || tick.symbol || "UNKNOWN").toString();

    logger.kafka("receive", `📥 Received tick for ${symbol}`, {
      partition,
      topic,
      tick: {
        price: tick.price,
        baseAsset: tick.baseAsset,
        quoteAsset: tick.quoteAsset,
        source: tick.source,
        ts: tick.ts,
      },
    });

    const stats = symbolStats.get(symbol) || {
      received: 0,
      processed: 0,
      lastPrice: 0,
    };
    stats.received++;
    symbolStats.set(symbol, stats);

    const beforeBuckets = getBucketStats().openBuckets;
    upsertTick(tick);
    const afterBuckets = getBucketStats().openBuckets;

    if (env.ALLOWED_SYMBOLS.length && !env.ALLOWED_SYMBOLS.includes(symbol)) {
      totalTicksSkipped++;
      logger.warn(`⏭️  SKIPPED tick for ${symbol} (not in ALLOWED_SYMBOLS)`);
    } else {
      totalTicksProcessed++;
      stats.processed++;
      stats.lastPrice = tick.price;
      symbolStats.set(symbol, stats);

      state.lastTickIso = new Date().toISOString();

      logger.info(`🧠 Processed tick for ${symbol} into OHLC windows`, {
        windows: env.WINDOWS,
        openBuckets: beforeBuckets,
        newBuckets: afterBuckets - beforeBuckets,
      });

      if (totalTicksReceived % 50 === 0) {
        logger.stats({
          totalTicksReceived,
          totalTicksProcessed,
          totalTicksSkipped,
        });
      }
    }
  } catch (e: any) {
    logger.error("Failed to parse/process tick", {
      error: e?.message,
      messageValue: message.value.toString(),
    });
  }
}

async function main() {
  logger.start("Starting analytics-prices...");

  logger.info("Step 1/4: Initializing Kafka (consumer + producer)...");
  await initKafka();
  logger.info(`✓ Kafka connected to: ${env.KAFKA_BROKERS.join(", ")}`);

  logger.info("Step 2/4: Starting health check server...");
  const app = createHealthServer(state);
  await app.listen({ port: 3102, host: "0.0.0.0" });
  logger.info("✓ Health server listening at http://0.0.0.0:3102");

  logger.info("Step 3/4: Subscribing to Kafka topic...");
  const consumer = getConsumer();
  await consumer.subscribe({ topic: env.PRICES_TOPIC, fromBeginning: false });
  logger.info(`✓ Subscribed to topic: ${env.PRICES_TOPIC}`);

  logger.info("Step 4/4: Starting Kafka consumer...");
  await consumer.run({ eachMessage: handleMessage });
  logger.info("✓ Kafka consumer running");

  printBanner(
    "ANALYTICS-PRICES",
    "Kafka (prices.raw.v1) → OHLC → Kafka (metrics.events)"
  );

  logger.info("✅ Analytics-prices started successfully!");
  logger.box("Configuration:", [
    `Input Topic: ${env.PRICES_TOPIC}`,
    `Output Topic: ${env.METRICS_TOPIC}`,
    `Windows: ${env.WINDOWS}`,
    `Flush Interval: ${env.FLUSH_INTERVAL_MS}ms`,
    `Allowed Symbols: ${env.ALLOWED_SYMBOLS.join(", ")}`,
    `Default Currency: ${env.DEFAULT_CURRENCY}`,
  ]);

  const FLUSH_INTERVAL = Math.min(env.FLUSH_INTERVAL_MS, 2000);
  logger.info(`⚡ Using optimized flush interval: ${FLUSH_INTERVAL}ms`);

  const timer = setInterval(async () => {
    try {
      const beforeStats = getBucketStats();
      const flushed = await flushDue();

      if (flushed) {
        totalFlushes++;
        totalWindowsFlushed += flushed;

        state.lastFlushIso = new Date().toISOString();
        const afterStats = getBucketStats();

        logger.info(`Flushed ${flushed} OHLC windows to Kafka`, {
          topic: env.METRICS_TOPIC,
          openBucketsBefore: beforeStats.openBuckets,
          openBucketsAfter: afterStats.openBuckets,
          totalFlushes,
          totalWindowsFlushed,
          lastFlushIso: state.lastFlushIso,
        });

        if (totalFlushes % 5 === 0) {
          console.log("");
          console.log("┌" + "─".repeat(78) + "┐");
          console.log(`│ 📊 SYMBOL STATISTICS${" ".repeat(57)}│`);
          console.log("├" + "─".repeat(78) + "┤");

          Array.from(symbolStats.entries())
            .sort((a, b) => b[1].processed - a[1].processed)
            .forEach(([sym, stats]) => {
              const rate = ((stats.processed / stats.received) * 100).toFixed(
                1
              );
              const symStr = sym.padEnd(8);
              const priceStr = `$${stats.lastPrice.toFixed(2)}`.padEnd(12);
              const processedStr = stats.processed.toString().padEnd(6);
              const receivedStr = stats.received.toString().padEnd(6);
              console.log(
                `│   💎 ${symStr} ${priceStr} | 📦 ${processedStr}/${receivedStr} (${rate}%)${" ".repeat(
                  Math.max(0, 15 - rate.length)
                )}│`
              );
            });

          console.log("└" + "─".repeat(78) + "┘");
          console.log("");
        }
      }
    } catch (e: any) {
      logger.error("Error during flush", {
        error: e?.message,
        stack: e?.stack,
      });
    }
  }, FLUSH_INTERVAL);

  const forceFlushTimer = setInterval(async () => {
    try {
      const stats = getBucketStats();
      if (stats.openBuckets > 0) {
        logger.info(
          `⚡ Force flushing ${stats.openBuckets} incomplete candle(s) for live view`
        );

        const flushed = await flushDue();

        if (flushed) {
          logger.info(`✓ Force flush completed: ${flushed} window(s) flushed`);
        } else {
          logger.debug("No candles ready for force flush");
        }
      }
    } catch (e: any) {
      logger.error("Error during force flush", {
        error: e?.message,
      });
    }
  }, 30000);

  for (const sig of ["SIGINT", "SIGTERM"] as const) {
    process.on(sig, async () => {
      if (!running) return;

      logger.warn(`Signal received: ${sig}, shutting down...`);
      running = false;
      clearInterval(timer);
      clearInterval(forceFlushTimer);

      logger.info("Performing final flush...");
      const flushed = await flushDue();
      if (flushed) {
        logger.info(`✓ Final flush completed: ${flushed} window(s)`);
      }

      logger.stats({
        totalTicksReceived,
        totalTicksProcessed,
        totalTicksSkipped,
        totalFlushes,
        totalWindowsFlushed,
        processRate: `${(
          (totalTicksProcessed / totalTicksReceived) *
          100
        ).toFixed(1)}%`,
        symbolStats: JSON.stringify(
          Array.from(symbolStats.entries()).map(([sym, stats]) => ({
            symbol: sym,
            received: stats.received,
            processed: stats.processed,
            lastPrice: stats.lastPrice,
          }))
        ),
      });

      await shutdownKafka();
      logger.info("✓ Shutdown complete");
      process.exit(0);
    });
  }
}

process.on("unhandledRejection", (reason) => {
  logger.error("Unhandled Rejection", {
    reason: reason instanceof Error ? reason.message : String(reason),
  });
});

process.on("uncaughtException", (error) => {
  logger.error("Uncaught Exception", {
    error: error.message,
    stack: error.stack,
  });
  process.exit(1);
});

main().catch(async (e) => {
  logger.error("Failed to start analytics-prices", {
    error: e?.message,
    stack: e?.stack,
  });
  await shutdownKafka();
  process.exit(1);
});
