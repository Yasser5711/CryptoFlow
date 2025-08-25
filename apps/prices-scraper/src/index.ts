import "dotenv/config";
import { initProducer, getProducer, shutdownProducer } from "./kafka";
import { env } from "./env";
import { fetchLiveTicks, shutdownBinanceWS } from "./binance-live";
import { createHealthServer } from "./health";
import { printBanner } from "@cryptoflow/banner";
import logger from "./logger";

let running = true;
let lastTickIso: string | null = null;
const state = { lastTickIso: null as string | null };

let totalTicksProcessed = 0;
let ticksThisMinute = 0;
let lastMinuteReset = Date.now();
const symbolStats = new Map<
  string,
  { count: number; lastPrice: number; lastTs: string }
>();

async function pollOnce() {
  try {
    const ticks = await fetchLiveTicks();

    if (ticks.length === 0) {
      return;
    }

    const producer = getProducer();
    const messages = ticks.map((t) => ({
      key: t.baseAsset,
      value: JSON.stringify(t),
    }));

    totalTicksProcessed += ticks.length;
    ticksThisMinute += ticks.length;

    for (const tick of ticks) {
      const symbol = tick.baseAsset;
      const existing = symbolStats.get(symbol);

      symbolStats.set(symbol, {
        count: (existing?.count || 0) + 1,
        lastPrice: tick.price,
        lastTs: tick.ts || new Date().toISOString(),
      });
    }

    logger.info(`📥 LIVE PRICE TICKS RECEIVED FROM BINANCE WEBSOCKET`);

    logger.info(
      `💰 Total: ${ticks.length} tick(s) | Running total: ${
        totalTicksProcessed + ticks.length
      }`
    );

    const startPublish = Date.now();
    await producer.send({
      topic: env.PRICES_TOPIC,
      messages,
    });
    const publishDuration = Date.now() - startPublish;

    lastTickIso = new Date().toISOString();
    state.lastTickIso = lastTickIso;

    logger.kafka("send", "Published to Kafka", {
      topic: env.PRICES_TOPIC,
      messages,
      duration: publishDuration,
    });

    const now = Date.now();
    if (now - lastMinuteReset >= 60000) {
      logger.stats({
        period: "1 minute",
        ticksProcessed: ticksThisMinute,
        symbolBreakdown: JSON.stringify(
          Array.from(symbolStats.entries()).map(([sym, stats]) => ({
            symbol: sym,
            count: stats.count,
            lastPrice: stats.lastPrice,
          }))
        ),
      });
      ticksThisMinute = 0;
      lastMinuteReset = now;
    }
  } catch (err: any) {
    logger.error("Error in pollOnce:", {
      error: err?.message,
      stack: err?.stack,
    });
  }
}

async function main() {
  logger.start("Starting prices-scraper...");

  logger.info("Step 1/3: Initializing Kafka producer...");
  await initProducer();
  logger.kafka("connect", "Kafka producer connected successfully");

  logger.info("Step 2/3: Starting health check server...");
  const app = createHealthServer(state);
  await app.listen({ port: 3101, host: "0.0.0.0" });
  logger.info("✓ Health server listening at http://0.0.0.0:3101");

  logger.info("Step 3/3: Configuration loaded");
  logger.info(`Target symbols: ${env.PRICE_SYMBOLS.join(", ")}`);
  logger.info(`Kafka topic: ${env.PRICES_TOPIC}`);
  logger.info(`Poll interval: ${env.POLL_INTERVAL_MS}ms`);

  printBanner("PRICES-SCRAPER", "Binance WebSocket → Kafka (prices.raw.v1)");

  logger.info("✅ Prices scraper started successfully!");
  logger.info("📡 Listening to Binance WebSocket for live price ticks...");

  while (running) {
    await pollOnce();
    await new Promise((r) => setTimeout(r, env.POLL_INTERVAL_MS));
  }
}

for (const sig of ["SIGINT", "SIGTERM"] as const) {
  process.on(sig, async () => {
    if (!running) return;

    logger.warn(`Signal received: ${sig}, shutting down...`);
    running = false;

    logger.stats({
      totalTicksProcessed,
      symbolStats: JSON.stringify(
        Array.from(symbolStats.entries()).map(([sym, stats]) => ({
          symbol: sym,
          totalCount: stats.count,
          lastPrice: stats.lastPrice,
          lastTs: stats.lastTs,
        }))
      ),
    });

    await shutdownBinanceWS();
    await shutdownProducer();

    logger.info("✓ Shutdown complete");
    process.exit(0);
  });
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
  logger.error("Failed to start prices-scraper", {
    error: e?.message,
    stack: e?.stack,
  });
  await shutdownBinanceWS();
  await shutdownProducer();
  process.exit(1);
});
