import { OhlcPoint, PriceTick } from "./types";
import { env } from "./env";
import { getProducer } from "./kafka";
/** Parse "1m|5m|15m|1h" -> { "1m":60000, "5m":300000, ... } */
function parseWindows(ws: string[]): Record<string, number> {
  const map: Record<string, number> = {};
  for (const w of ws) {
    const m = w.match(/^(\d+)([mhd])$/i);
    if (!m) continue;
    const n = Number(m[1]);
    const unit = m[2].toLowerCase();
    const mult = unit === "m" ? 60_000 : unit === "h" ? 3_600_000 : 86_400_000;
    map[w] = n * mult;
  }
  if (!map["1m"]) map["1m"] = 60_000;
  return map;
}

const WIN_MS = parseWindows(env.WINDOWS);

/** key = `${win}|${coin}|${currency}|${bucketStartMs}` */
const buckets = new Map<
  string,
  OhlcPoint & { win: string; winMs: number; source: string }
>();

function bucketStart(ms: number, winMs: number) {
  return Math.floor(ms / winMs) * winMs;
}

export function upsertTick(tick: PriceTick) {
  let coin = (tick.baseAsset || "")?.toUpperCase();
  if (!coin && tick.symbol) {
    const s = tick.symbol.toUpperCase();
    const QUOTES = ["USDT", "BUSD", "USDC", "USD", "EUR", "BTC", "ETH", "BNB"];
    const q = QUOTES.find((q) => s.endsWith(q));
    if (q) coin = s.slice(0, s.length - q.length);
  }
  if (!coin) return;

  if (env.ALLOWED_SYMBOLS.length && !env.ALLOWED_SYMBOLS.includes(coin)) return;

  const price = Number(tick.price);
  if (!Number.isFinite(price)) return;

  const currency = (
    tick.quoteAsset ||
    tick.currency ||
    env.DEFAULT_CURRENCY
  ).toUpperCase();
  const src = (tick.source ?? "unknown").toLowerCase();
  const tickMs = tick.ts ? Date.parse(tick.ts) : Date.now();

  for (const [win, winMs] of Object.entries(WIN_MS)) {
    const bStart = bucketStart(tickMs, winMs);
    const key = `${win}|${coin}|${currency}|${bStart}`;

    const prev = buckets.get(key);
    if (!prev) {
      buckets.set(key, {
        win,
        winMs,
        source: src,
        open: price,
        high: price,
        low: price,
        close: price,
        sum: price,
        count: 1,
        bucketStartMs: bStart,
      } as any);
    } else {
      prev.high = Math.max(prev.high, price);
      prev.low = Math.min(prev.low, price);
      prev.close = price;
      prev.sum += price;
      prev.count += 1;
      prev.source = src;
    }
  }
}

export async function flushDue(nowMs = Date.now()) {
  if (!buckets.size) return;

  const due: Array<
    [string, OhlcPoint & { win: string; winMs: number; source: string }]
  > = [];
  for (const [key, b] of buckets) {
    if (nowMs >= b.bucketStartMs + b.winMs) {
      due.push([key, b]);
      buckets.delete(key);
    }
  }
  if (!due.length) return;

  const producer = getProducer();
  const messages = due.map(([key, b]) => {
    const [, coin, currency] = key.split("|") as [
      string,
      string,
      string,
      string
    ];
    const avg = b.sum / b.count;
    const tsIso = new Date(b.bucketStartMs + b.winMs).toISOString();

    const evt = {
      measurement: "price",
      tags: { coin, currency, window: b.win, source: b.source || "binance" },
      fields: {
        open: b.open,
        high: b.high,
        low: b.low,
        close: b.close,
        avg,
        count: b.count,
      },
      ts: tsIso,
    };

    return { key: coin, value: JSON.stringify(evt) };
  });

  await producer.send({ topic: env.METRICS_TOPIC, messages });
  return due.length;
}

export function getBucketStats() {
  return { openBuckets: buckets.size };
}
