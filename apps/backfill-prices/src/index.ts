import "dotenv/config";
import { env, resolveTimeRange, getSymbols } from "./env";
import { fetchKlinesPage, splitSymbol, type Kline } from "./binance";
import { getProducer, initProducer, shutdownProducer } from "./kafka";
import { getPresentMinuteSet } from "./influx";
import { printBanner } from "@cryptoflow/banner";
import logger from "./logger";
type PriceEvent = {
  measurement: "price";
  tags: {
    coin: string;
    currency: string;
    window: "1m";
    source: string;
  };
  fields: {
    open: number;
    high: number;
    low: number;
    close: number;
    avg: number;
    count: number;
  };
  ts: string;
};

function toNum(s: string) {
  const n = Number(s);
  if (!Number.isFinite(n)) throw new Error(`NaN from ${s}`);
  return n;
}

function klineToEvent(k: Kline, coin: string, currency: string): PriceEvent {
  const openTime = k[0];
  const open = toNum(k[1]);
  const high = toNum(k[2]);
  const low = toNum(k[3]);
  const close = toNum(k[4]);
  const avg = (open + high + low + close) / 4;
  const ts = new Date(openTime + 60_000).toISOString();

  return {
    measurement: "price",
    tags: { coin, currency, window: "1m", source: "binance" },
    fields: { open, high, low, close, avg, count: 1 },
    ts,
  };
}

async function sleep(ms: number) {
  if (ms > 0) return new Promise((r) => setTimeout(r, ms));
}
async function backfillOneSymbol(
  symbol: string,
  startMs: number,
  endMs: number
) {
  const { coin, quote } = splitSymbol(symbol);
  const producer = getProducer();

  logger.info(
    `${symbol} 1m from ${new Date(startMs).toISOString()} to ${new Date(
      endMs
    ).toISOString()} (limit=${env.LIMIT})`
  );

  let cursor = startMs;
  let total = 0;
  let skipped = 0;
  while (cursor < endMs) {
    const pageEnd = Math.min(endMs, cursor + env.LIMIT * 60_000);
    let klines: Kline[] = [];
    try {
      klines = await fetchKlinesPage({
        symbol,
        interval: "1m",
        startTime: cursor,
        endTime: pageEnd - 1,
        limit: env.LIMIT,
      });
    } catch (e: any) {
      logger.warn(`fetch error ${symbol}: ${e?.message ?? e}`);
      await sleep(1000);
      continue;
    }

    if (klines.length === 0) {
      cursor = pageEnd;
      await sleep(env.SLEEP_MS);
      continue;
    }

    const baseEvents = klines.map((k) => klineToEvent(k, coin, quote));

    const out: PriceEvent[] = [];
    for (const ev of baseEvents) {
      const baseQuote = ev.tags.currency;

      if (baseQuote === "EUR" && env.FIATS.includes("EUR")) out.push(ev);

      if (baseQuote === "USDT" && env.FIATS.includes("USD")) {
        out.push({ ...ev, tags: { ...ev.tags, currency: "USD" } });
      }
    }

    const presentUSD = env.FIATS.includes("USD")
      ? await getPresentMinuteSet({
          coin,
          currency: "USD",
          startMs: cursor + 60_000,
          endMs: pageEnd,
        })
      : new Set<number>();
    const presentEUR = env.FIATS.includes("EUR")
      ? await getPresentMinuteSet({
          coin,
          currency: "EUR",
          startMs: cursor + 60_000,
          endMs: pageEnd,
        })
      : new Set<number>();

    const filtered = out.filter((evt) => {
      const t = Date.parse(evt.ts);
      const set = evt.tags.currency === "USD" ? presentUSD : presentEUR;
      const keep = !set.has(t);
      if (!keep) skipped++;
      return keep;
    });
    const messages = filtered.map((evt) => ({
      key: coin,
      value: JSON.stringify(evt),
    }));
    if (messages.length) {
      await producer.send({ topic: env.METRICS_TOPIC, messages });
      total += messages.length;
    }
    const lastOpenTime = klines[klines.length - 1][0];
    cursor = lastOpenTime + 60_000;

    logger.kafka(
      "send",
      `${symbol} sent ${
        messages.length
      } bar(s); skipped=${skipped} ; cursor=${new Date(
        cursor
      ).toISOString()} total=${total}`
    );
    await sleep(env.SLEEP_MS);
  }

  logger.success(`${symbol} done. total produced=${total}, skipped=${skipped}`);
  logger.stats({
    symbol,
    totalProduced: total,
    totalSkipped: skipped,
  });
}
async function main() {
  printBanner("Backfill Prices", "1.0.0");
  const { startMs, endMs } = resolveTimeRange();
  const symbols = getSymbols();

  await initProducer();
  try {
    for (const sym of symbols) {
      await backfillOneSymbol(sym, startMs, endMs);
    }
  } finally {
    await shutdownProducer();
  }
}

for (const sig of ["SIGINT", "SIGTERM"] as const) {
  process.on(sig, async () => {
    try {
      await shutdownProducer();
    } finally {
      process.exit(0);
    }
  });
}

main().catch(async (e) => {
  logger.error(e);
  await shutdownProducer();
  process.exit(1);
});
