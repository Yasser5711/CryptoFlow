import type { FastifyInstance, FastifyReply } from "fastify";
import { queryCandles } from "../services/influx.js";
import { env } from "../env";
import logger from "./logger";

interface PriceTick {
  source: string;
  symbol?: string;
  baseAsset?: string;
  quoteAsset?: string;
  price: number;
  ts?: string;
}

interface CachedPrice {
  price: number;
  ts: string;
  source: string;
  currency: string;
}

interface LiveCandle {
  open: number;
  high: number;
  low: number;
  close: number;
  count: number;
  bucketStartMs: number;
  lastUpdate: number;
}

interface SSEClient {
  reply: FastifyReply;
  coin: string;
  currency: string;
  window: string;
  startTime: number;
  lastSent: number;
  messageCount: number;
}
type Finalized = LiveCandle & { expiresAt: number };
const finalizedCandles = new Map<string, Finalized>();

const FINALIZED_TTL = 2 * 60_000;

const latestPrices = new Map<string, CachedPrice>();
const liveCandles = new Map<string, LiveCandle>();
const activeClients = new Map<string, SSEClient>();

const THROTTLE_INTERVAL = 2000;
const lastBroadcastTime = new Map<string, number>();

let kafkaListenerActive = false;
let kafkaMessageCount = 0;
let lastKafkaMessageTime: Date | null = null;

function getCacheKey(coin: string, currency: string): string {
  return `${coin.toUpperCase()}:${currency.toUpperCase()}`;
}
function getLiveCandleKey(
  coin: string,
  currency: string,
  window: string,
  bucketStartMs: number
): string {
  return `${coin.toUpperCase()}:${currency.toUpperCase()}:${window}:${bucketStartMs}`;
}
function getWindowMs(window: string): number {
  switch (window) {
    case "5m":
      return 300_000;
    case "15m":
      return 900_000;
    case "1h":
      return 3_600_000;
    case "1m":
    default:
      return 60_000;
  }
}
function getBucketStart(ms: number, winMs: number): number {
  return Math.floor(ms / winMs) * winMs;
}
function generateClientId(): string {
  return `client_${Date.now()}_${Math.random().toString(36).slice(2, 11)}`;
}
function cleanupFinalizedCandles() {
  const now = Date.now();
  for (const [key, candle] of finalizedCandles.entries()) {
    if (now > candle.expiresAt) finalizedCandles.delete(key);
  }
}

function updateLiveCandles(
  coin: string,
  currency: string,
  price: number,
  ts: string
) {
  const tickMs = Date.parse(ts);
  for (const window of ["1m", "5m", "15m", "1h"] as const) {
    const winMs = getWindowMs(window);
    const bucketStartMs = getBucketStart(tickMs, winMs);
    const prevBucketStartMs = bucketStartMs - winMs;
    const prevKey = getLiveCandleKey(coin, currency, window, prevBucketStartMs);
    const prevCandle = liveCandles.get(prevKey);

    if (prevCandle) {
      liveCandles.delete(prevKey);
      finalizedCandles.set(prevKey, {
        ...prevCandle,
        expiresAt: Date.now() + FINALIZED_TTL,
      });

      for (const client of activeClients.values()) {
        if (
          client.coin === coin.toUpperCase() &&
          client.currency === currency.toUpperCase() &&
          client.window === window
        ) {
          sendSSEMessage(client, "finalCandle", {
            coin,
            currency,
            window,
            bucketStart: new Date(prevCandle.bucketStartMs).toISOString(),
            open: prevCandle.open,
            high: prevCandle.high,
            low: prevCandle.low,
            close: prevCandle.close,
            count: prevCandle.count,
          });
        }
      }
    }
    const key = getLiveCandleKey(coin, currency, window, bucketStartMs);
    const cur = liveCandles.get(key);

    if (!cur) {
      liveCandles.set(key, {
        open: price,
        high: price,
        low: price,
        close: price,
        count: 1,
        bucketStartMs,
        lastUpdate: tickMs,
      });
      logger.info(`📊 New live candle ${key}`, {
        bucketStartIso: new Date(bucketStartMs).toISOString(),
      });
    } else {
      cur.high = Math.max(cur.high, price);
      cur.low = Math.min(cur.low, price);
      cur.close = price;
      cur.count += 1;
      cur.lastUpdate = tickMs;
    }
  }

  const now = Date.now();
  let cleaned = 0;
  for (const [k, c] of liveCandles.entries()) {
    const [, , window] = k.split(":");
    const winMs = getWindowMs(window);
    if (now > c.bucketStartMs + winMs + 60_000) {
      liveCandles.delete(k);
      cleaned++;
    }
  }
  if (cleaned) {
    logger.debug(`🧹 Cleaned ${cleaned} completed live candle(s)`);
  }
  cleanupFinalizedCandles();
}

function updateLatestPrice(
  coin: string,
  currency: string,
  price: number,
  ts: string,
  source: string
) {
  const key = getCacheKey(coin, currency);
  const cached = latestPrices.get(key);
  if (!cached || new Date(ts) >= new Date(cached.ts)) {
    latestPrices.set(key, { price, ts, source, currency });
    logger.price(`${coin}/${currency}`, price, currency);
    updateLiveCandles(coin, currency, price, ts);
    broadcastPriceUpdate(coin, currency, price, ts, source);
  } else {
    logger.debug(`⏭️  Stale price ignored for ${key}`, {
      receivedTs: ts,
      cachedTs: cached.ts,
    });
  }
}

function broadcastPriceUpdate(
  coin: string,
  currency: string,
  price: number,
  ts: string,
  source: string
) {
  const key = getCacheKey(coin, currency);
  const now = Date.now();
  const last = lastBroadcastTime.get(key) ?? 0;
  if (now - last < THROTTLE_INTERVAL) return;
  lastBroadcastTime.set(key, now);

  let sent = 0;
  const dropped: string[] = [];
  for (const [id, client] of activeClients.entries()) {
    if (getCacheKey(client.coin, client.currency) !== key) continue;

    const okTick = sendSSEMessage(client, "tick", {
      coin,
      currency,
      price,
      ts,
      source,
    });

    const liveC = getLiveCandle(coin, currency, client.window);
    const okCandle = liveC
      ? sendSSEMessage(client, "liveCandle", {
          coin,
          currency,
          window: client.window,
          bucketStart: new Date(liveC.bucketStartMs).toISOString(),
          open: liveC.open,
          high: liveC.high,
          low: liveC.low,
          close: liveC.close,
          count: liveC.count,
          lastUpdate: new Date(liveC.lastUpdate).toISOString(),
        })
      : true;

    if (okTick && okCandle) sent++;
    else dropped.push(id);
  }

  for (const id of dropped) removeSSEClient(id);
  if (sent) {
    logger.ws(
      "message",
      `📡 Broadcast ${coin}/${currency} to ${sent} client(s)`,
      {
        price,
        ts,
        source,
      }
    );
  }
}

function getLiveCandle(
  coin: string,
  currency: string,
  window: string
): LiveCandle | null {
  const now = Date.now();
  const ms = getWindowMs(window);
  const bucketStartMs = getBucketStart(now, ms);
  const key = getLiveCandleKey(coin, currency, window, bucketStartMs);
  return liveCandles.get(key) ?? null;
}

function addSSEClient(id: string, client: SSEClient) {
  activeClients.set(id, client);
  logger.ws("connect", `SSE client connected: ${id}`, {
    coin: client.coin,
    currency: client.currency,
    window: client.window,
    totalClients: activeClients.size,
  });
}
function removeSSEClient(id: string) {
  const c = activeClients.get(id);
  if (!c) return;
  activeClients.delete(id);
  logger.ws("disconnect", `SSE client disconnected: ${id}`, {
    coin: c.coin,
    currency: c.currency,
    durationSec: ((Date.now() - c.startTime) / 1000).toFixed(1),
    messagesSent: c.messageCount,
    totalClients: activeClients.size,
  });
}
function sendSSEMessage(client: SSEClient, event: string, data: any): boolean {
  try {
    client.reply.raw.write(
      `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`
    );
    client.messageCount++;
    client.lastSent = Date.now();
    return true;
  } catch (e) {
    logger.error("Failed to send SSE message", {
      event,
      error: (e as Error).message,
    });
    return false;
  }
}
function getClientsByPair(): Record<string, number> {
  const res: Record<string, number> = {};
  for (const c of activeClients.values()) {
    const k = `${c.coin}/${c.currency}/${c.window}`;
    res[k] = (res[k] || 0) + 1;
  }
  return res;
}

export async function initializeKafkaListener() {
  if (kafkaListenerActive) {
    logger.warn("Kafka prices listener already active");
    return;
  }

  const PRICES_TOPIC = env.PRICES_TOPIC;
  if (!PRICES_TOPIC) {
    throw new Error("PRICES_TOPIC is not set");
  }

  const brokers = env.KAFKA_BROKERS.split(",")
    .map((b) => b.trim())
    .filter(Boolean);
  const { Kafka, logLevel } = await import("kafkajs");

  const kafka = new Kafka({
    clientId: `${env.KAFKA_CLIENT_ID}-prices-monitor`,
    brokers,
    logLevel: logLevel.WARN,
    retry: { retries: 10 },
  });

  const consumer = kafka.consumer({
    groupId: `${env.KAFKA_GROUP_ID}-prices-monitor`,
    sessionTimeout: 20_000,
    heartbeatInterval: 3000,
  });

  try {
    logger.kafka("connect", "Connecting Kafka prices consumer…", {
      brokers,
      topic: PRICES_TOPIC,
    });
    await consumer.connect();
    await consumer.subscribe({ topic: PRICES_TOPIC, fromBeginning: false });
    logger.kafka("connect", "✓ Kafka prices consumer connected & subscribed", {
      topic: PRICES_TOPIC,
    });

    await consumer.run({
      eachMessage: async ({ message, partition }) => {
        if (!message.value) return;
        try {
          const tick: PriceTick = JSON.parse(message.value.toString());
          kafkaMessageCount++;
          lastKafkaMessageTime = new Date();

          const coin = (
            tick.baseAsset ||
            tick.symbol ||
            "UNKNOWN"
          ).toUpperCase();
          const currency = (tick.quoteAsset || "USDT").toUpperCase();
          const price = Number(tick.price);
          const ts = tick.ts || new Date().toISOString();
          const source = tick.source || "binance";

          if (!coin || !Number.isFinite(price)) return;

          if (kafkaMessageCount % 50 === 1) {
            logger.info("📥 Live price from Kafka", {
              coin,
              currency,
              price,
              ts,
              partition,
            });
          }

          updateLatestPrice(coin, currency, price, ts, source);

          if (currency === "USDT") {
            updateLatestPrice(coin, "USD", price, ts, "derived");
            const fx = Number(env.FX_USD_TO_EUR);
            if (Number.isFinite(fx) && fx > 0) {
              updateLatestPrice(coin, "EUR", price * fx, ts, "derived");
            }
          }
        } catch (e) {
          logger.error("Kafka message parse error", {
            error: (e as Error).message,
          });
        }
      },
    });

    kafkaListenerActive = true;
    logger.success("✅ Kafka prices listener started");
  } catch (e) {
    kafkaListenerActive = false;
    logger.error("Failed to start Kafka prices listener", {
      error: (e as Error).message,
    });
    throw e;
  }
}

export async function registerRealTimeServer(fastify: FastifyInstance) {
  logger.start("📡 Registering real-time streaming endpoints…");

  fastify.options("/api/stream/prices/:coin", async (req, reply) => {
    const origin = req.headers.origin || "*";
    reply
      .header("Access-Control-Allow-Origin", origin)
      .header("Access-Control-Allow-Credentials", "true")
      .header("Access-Control-Allow-Methods", "GET, OPTIONS")
      .header(
        "Access-Control-Allow-Headers",
        "Content-Type, Authorization, X-Requested-With, Cache-Control"
      )
      .header("Access-Control-Max-Age", "86400");
    return reply.code(204).send();
  });

  fastify.get("/api/stream/prices/:coin", async (request, reply) => {
    const { coin } = request.params as { coin: string };
    const { currency = "USD", window = "1m" } = request.query as {
      currency?: string;
      window?: string;
    };
    const origin = request.headers.origin || "http://localhost:5173";

    reply.raw.setHeader("Content-Type", "text/event-stream");
    reply.raw.setHeader("Cache-Control", "no-cache, no-transform");
    reply.raw.setHeader("Connection", "keep-alive");
    reply.raw.setHeader("Access-Control-Allow-Origin", origin);
    reply.raw.setHeader("Access-Control-Allow-Credentials", "true");
    reply.raw.setHeader("Access-Control-Expose-Headers", "Content-Type");
    reply.raw.setHeader("X-Accel-Buffering", "no");
    reply.hijack();

    const clientId = generateClientId();
    const client: SSEClient = {
      reply,
      coin: coin.toUpperCase(),
      currency: (currency || "USD").toUpperCase(),
      window: window || "1m",
      startTime: Date.now(),
      lastSent: Date.now(),
      messageCount: 0,
    };

    addSSEClient(clientId, client);

    sendSSEMessage(client, "connected", {
      coin: client.coin,
      currency: client.currency,
      window: client.window,
      clientId,
      timestamp: new Date().toISOString(),
      kafkaActive: kafkaListenerActive,
    });

    const latest = latestPrices.get(getCacheKey(client.coin, client.currency));
    if (latest) {
      logger.info(
        `📤 Initial tick to ${clientId}: ${client.coin}/${
          client.currency
        } $${latest.price.toFixed(2)}`
      );
      sendSSEMessage(client, "tick", { coin: client.coin, ...latest });

      const live = getLiveCandle(client.coin, client.currency, client.window);
      if (live) {
        sendSSEMessage(client, "liveCandle", {
          coin: client.coin,
          currency: client.currency,
          window: client.window,
          bucketStart: new Date(live.bucketStartMs).toISOString(),
          open: live.open,
          high: live.high,
          low: live.low,
          close: live.close,
          count: live.count,
          lastUpdate: new Date(live.lastUpdate).toISOString(),
        });
      }
    } else {
      logger.warn(`No cached price yet for ${client.coin}/${client.currency}`);
      sendSSEMessage(client, "info", { message: "Waiting for price data…" });
    }

    const ping = setInterval(() => {
      const ok = sendSSEMessage(client, "ping", {
        timestamp: new Date().toISOString(),
        uptime: Date.now() - client.startTime,
        messagesSent: client.messageCount,
      });
      if (!ok) {
        clearInterval(ping);
        removeSSEClient(clientId);
      }
    }, 30_000);

    reply.raw.on("close", () => {
      clearInterval(ping);
      removeSSEClient(clientId);
    });
    reply.raw.on("error", (err) => {
      clearInterval(ping);
      logger.error(`SSE client error: ${clientId}`, { error: err?.message });
      removeSSEClient(clientId);
    });
  });

  fastify.get("/api/live-candles/:coin", async (request, reply) => {
    const { coin } = request.params as { coin: string };
    const {
      window = "1m",
      range = "-6h",
      currency = "USD",
      source,
    } = request.query as Record<string, string>;

    logger.info(`📊 Live candles request: ${coin}`, {
      window,
      range,
      currency,
      source,
    });

    try {
      const candles = await queryCandles({
        measurement: "price",
        coin: coin.toUpperCase(),
        window: (window || "1m") as "1m" | "5m" | "15m" | "1h",
        range: range || "-6h",
        currency: currency?.toUpperCase(),
        source,
        limit: 2000,
      });
      const win = (window || "1m") as "1m" | "5m" | "15m" | "1h";
      const have = new Set(candles.map((c: any) => c._time));

      const extra: any[] = [];
      for (const [key, c] of finalizedCandles.entries()) {
        const [cc, cur, ww] = key.split(":");
        if (
          cc === coin.toUpperCase() &&
          cur === currency.toUpperCase() &&
          ww === win
        ) {
          const iso = new Date(c.bucketStartMs).toISOString();
          if (!have.has(iso)) {
            extra.push({
              _time: iso,
              coin: cc,
              currency: cur,
              window: ww,
              open: c.open,
              high: c.high,
              low: c.low,
              close: c.close,
              provenance: "finalized-cache",
            });
          }
        }
      }

      const merged = [...candles, ...extra].sort(
        (a, b) => new Date(a._time).getTime() - new Date(b._time).getTime()
      );

      const latest =
        latestPrices.get(
          getCacheKey(coin.toUpperCase(), currency.toUpperCase())
        ) || null;
      const live = getLiveCandle(
        coin.toUpperCase(),
        currency.toUpperCase(),
        window
      );

      return reply
        .header("Cache-Control", "public, max-age=2")
        .header("X-Total-Candles", candles.length.toString())
        .header("X-Has-Live-Price", latest ? "true" : "false")
        .header("X-Has-Live-Candle", live ? "true" : "false")
        .header("X-Kafka-Active", kafkaListenerActive ? "true" : "false")
        .send({
          candles: merged,
          latest,
          liveCandle: live
            ? {
                bucketStart: new Date(live.bucketStartMs).toISOString(),
                open: live.open,
                high: live.high,
                low: live.low,
                close: live.close,
                count: live.count,
                lastUpdate: new Date(live.lastUpdate).toISOString(),
              }
            : null,
          metadata: {
            coin: coin.toUpperCase(),
            currency: currency.toUpperCase(),
            window,
            range,
            totalCandles: candles.length,
            timestamp: new Date().toISOString(),
            kafkaActive: kafkaListenerActive,
            kafkaMessages: kafkaMessageCount,
            activeClients: activeClients.size,
            cachedPrices: latestPrices.size,
            liveCandlesInBuffer: liveCandles.size,
          },
        });
    } catch (e) {
      logger.error("Failed to fetch live candles", {
        error: (e as Error).message,
        coin,
        window,
        range,
      });
      return reply.code(500).send({
        error: "Failed to fetch live candles",
        message: (e as Error).message,
        timestamp: new Date().toISOString(),
      });
    }
  });

  fastify.get("/api/stream/status", async (request, reply) => {
    const status = {
      status: "ok",
      kafka: {
        active: kafkaListenerActive,
        messagesProcessed: kafkaMessageCount,
        lastMessageTime: lastKafkaMessageTime?.toISOString() || null,
        topic: env.PRICES_TOPIC,
      },
      cache: {
        pricesStored: latestPrices.size,
        liveCandlesInBuffer: liveCandles.size,
        coins: Array.from(latestPrices.keys()),
        details: Array.from(latestPrices.entries()).map(([key, data]) => ({
          pair: key,
          price: data.price,
          source: data.source,
          ageMs: Date.now() - new Date(data.ts).getTime(),
        })),
      },
      clients: {
        activeConnections: activeClients.size,
        byPair: getClientsByPair(),
        connections: Array.from(activeClients.values()).map((c) => ({
          coin: c.coin,
          currency: c.currency,
          window: c.window,
          uptimeMs: Date.now() - c.startTime,
          messagesSent: c.messageCount,
        })),
      },
      performance: {
        throttleIntervalMs: THROTTLE_INTERVAL,
        avgMessagesPerClient:
          activeClients.size > 0
            ? Math.round(
                Array.from(activeClients.values()).reduce(
                  (sum, c) => sum + c.messageCount,
                  0
                ) / activeClients.size
              )
            : 0,
      },
      timestamp: new Date().toISOString(),
    };

    logger.debug("Status requested", { from: request.ip });
    return reply.send(status);
  });

  logger.success("✅ Real-time streaming endpoints ready");
}

export function getStreamingStats() {
  return {
    kafkaActive: kafkaListenerActive,
    kafkaMessageCount,
    lastKafkaMessageTime,
    cachedPrices: latestPrices.size,
    liveCandlesInBuffer: liveCandles.size,
    activeClients: activeClients.size,
    cacheKeys: Array.from(latestPrices.keys()),
  };
}
