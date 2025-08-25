export const env = {
  INFLUX_URL: process.env.INFLUX_URL!,
  INFLUX_TOKEN: process.env.INFLUX_TOKEN!,
  INFLUX_ORG: process.env.INFLUX_ORG!,
  INFLUX_BUCKET: process.env.INFLUX_BUCKET!,
  KAFKA_BROKERS: process.env
    .KAFKA_BROKERS!.split(",")
    .map((s) => s.trim())
    .filter(Boolean),
  METRICS_TOPIC: process.env.METRICS_TOPIC!,

  BINANCE_BASE: process.env.BINANCE_BASE!,
  SYMBOLS: (process.env.BACKFILL_SYMBOLS ?? "")
    .split(",")
    .map((s) => s.trim().toUpperCase())
    .filter(Boolean),

  INTERVAL: process.env.BACKFILL_INTERVAL!.toLowerCase(),
  LIMIT: Number(process.env.BACKFILL_LIMIT!),
  SLEEP_MS: Number(process.env.BACKFILL_SLEEP_MS!),

  DAYS: process.env.BACKFILL_DAYS
    ? Number(process.env.BACKFILL_DAYS)
    : undefined,
  START_ISO: process.env.BACKFILL_START_ISO!,
  END_ISO: process.env.BACKFILL_END_ISO!,

  FIATS: (process.env.BACKFILL_FIATS ?? "USD")
    .split(",")
    .map((s) => s.trim().toUpperCase())
    .filter(Boolean),
};

function toMs(iso?: string): number | undefined {
  if (!iso) return undefined;
  const t = Date.parse(iso);
  return Number.isFinite(t) ? t : undefined;
}

export function resolveTimeRange(nowMs = Date.now()): {
  startMs: number;
  endMs: number;
} {
  if (env.INTERVAL !== "1m") {
    throw new Error(
      `Only 1m interval supported for backfill (got ${env.INTERVAL})`
    );
  }
  const nowFloor = Math.floor(nowMs / 60_000) * 60_000;

  if (typeof env.DAYS === "number" && !Number.isNaN(env.DAYS)) {
    const startMs = nowFloor - env.DAYS * 24 * 60 * 60 * 1000;
    const endMs = nowFloor - 60_000;
    return { startMs, endMs };
  }

  const startMs = toMs(env.START_ISO);

  const endMs = (toMs(env.END_ISO) ?? nowFloor) - 60_000;

  if (!startMs) {
    throw new Error(
      "Provide BACKFILL_DAYS or BACKFILL_START_ISO (and optionally BACKFILL_END_ISO)."
    );
  }
  if (startMs >= endMs) {
    throw new Error("Time range invalid: START >= END");
  }
  return { startMs, endMs };
}
export function getSymbols(): string[] {
  if (env.SYMBOLS.length) return env.SYMBOLS;

  throw new Error(
    "Provide BACKFILL_SYMBOLS (comma-separated) or BACKFILL_SYMBOL."
  );
}
