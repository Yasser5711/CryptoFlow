import { TIMEZONE } from "./constants";

export function formatDateTime(dt: Date) {
  return new Intl.DateTimeFormat("fr-FR", {
    timeZone: TIMEZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).format(dt);
}

export function formatPrice(price: number, currency: string) {
  try {
    return new Intl.NumberFormat("en-US", {
      style: "currency",
      currency,
      minimumFractionDigits: 2,
      maximumFractionDigits: 6,
    }).format(price);
  } catch {
    return `${price.toFixed(6)} ${currency}`;
  }
}

export function getWindowMs(window: string): number {
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
export function getBucketStart(ms: number, winMs: number): number {
  return Math.floor(ms / winMs) * winMs;
}

export function toCandleSeriesFromArray(
  arr: Array<
    | [number, number, number, number, number]
    | {
        timestamp: number;
        open: number;
        close: number;
        low: number;
        high: number;
      }
  >
) {
  return (arr as any[]).map((c) =>
    Array.isArray(c) ? c : [c.timestamp, c.open, c.close, c.low, c.high]
  );
}

export function SMA(series: number[][], period: number) {
  const out: [number, number | null][] = [];
  let sum = 0;
  const q: number[] = [];
  for (let i = 0; i < series.length; i++) {
    const t = series[i][0];
    const close = series[i][2];
    q.push(close);
    sum += close;
    if (q.length > period) sum -= q.shift()!;
    out.push([t, q.length === period ? sum / period : null]);
  }
  return out;
}
