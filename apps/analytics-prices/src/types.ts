export type PriceTick = {
  source: string; // "coingecko" (deprecated) or "binance"
  symbol?: string; // e.g. "BTC" (old format)
  baseAsset?: string; // e.g. "BTC" (new Binance format)
  quoteAsset?: string; // e.g. "USDT" (Binance format)
  currency?: string; // e.g. "USD", "USDT"
  price: number;
  ts?: string; // ISO-8601 (UTC). Optional: fallback to now.
};

export type BinanceTick = {
  source: string; // "binance"
  baseAsset: string; // e.g. "BTC"
  quoteAsset: string; // e.g. "USDT"
  price: number;
  ts?: string; // ISO-8601 (UTC)
};

export type OhlcPoint = {
  open: number;
  high: number;
  low: number;
  close: number;
  sum: number;
  count: number;
  bucketStartMs: number; // minute-start epoch ms
};
