import { env } from "./env";
import logger from "./logger";
export type Kline = [
  number, // openTime ms
  string, // open
  string, // high
  string, // low
  string, // close
  string, // volume
  number, // closeTime ms
  string, // quote asset volume
  number, // number of trades
  string, // taker buy base
  string, // taker buy quote
  string // ignore
];

export async function fetchKlinesPage(params: {
  symbol: string;
  interval: "1m";
  startTime: number;
  endTime?: number;
  limit: number;
}): Promise<Kline[]> {
  const u = new URL(`${env.BINANCE_BASE}/api/v3/klines`);
  u.searchParams.set("symbol", params.symbol);
  u.searchParams.set("interval", params.interval);
  u.searchParams.set("limit", String(Math.min(params.limit, 1000)));
  u.searchParams.set("startTime", String(params.startTime));
  if (params.endTime) u.searchParams.set("endTime", String(params.endTime));

  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 20000);

    const res = await fetch(u.toString(), {
      method: "GET",
      signal: controller.signal,
    });

    clearTimeout(timeout);

    if (!res.ok) {
      if (res.status === 429) {
        const retryAfter = res.headers.get("retry-after");
        const waitMs = retryAfter ? parseInt(retryAfter) * 1000 : 60000;
        throw new Error(
          `Binance rate limited (429). Wait ${waitMs}ms before retry`
        );
      }
      const text = await res.text().catch(() => "");
      throw new Error(`Binance HTTP ${res.status} ${res.statusText} – ${text}`);
    }

    const data = (await res.json()) as Kline[];
    if (!Array.isArray(data)) {
      logger.warn(
        `[backfill-prices] Unexpected Binance klines response for ${
          params.symbol
        }: ${JSON.stringify(data)}`
      );
      return [];
    }
    return data;
  } catch (e: any) {
    if (e?.name === "AbortError") {
      throw new Error("Binance request timeout (20s)");
    }
    logger.error(
      `[backfill-prices] Failed to fetch klines for ${params.symbol}: ${
        e?.message ?? e
      }`
    );
    throw e;
  }
}

export function splitSymbol(symbol: string): { coin: string; quote: string } {
  const QUOTES = ["USDT", "BUSD", "USDC", "USD", "EUR", "BTC", "ETH"];
  for (const q of QUOTES) {
    if (symbol.endsWith(q)) {
      return { coin: symbol.slice(0, symbol.length - q.length), quote: q };
    }
  }

  return { coin: symbol.slice(0, -3), quote: symbol.slice(-3) };
}
