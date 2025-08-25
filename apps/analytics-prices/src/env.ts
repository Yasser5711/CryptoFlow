export const env = {
  KAFKA_BROKERS: process.env
    .KAFKA_BROKERS!.split(",")
    .map((s) => s.trim())
    .filter(Boolean),
  PRICES_TOPIC: process.env.PRICES_TOPIC!,
  METRICS_TOPIC: process.env.METRICS_TOPIC!,
  WINDOWS: (process.env.WINDOWS ?? "1m").split(",").map((s) => s.trim()),
  FLUSH_INTERVAL_MS: Number(process.env.FLUSH_INTERVAL_MS ?? "10000"),
  ALLOWED_SYMBOLS: (process.env.ALLOWED_SYMBOLS ?? "")
    .split(",")
    .map((s) => s.trim().toUpperCase())
    .filter(Boolean),
  DEFAULT_CURRENCY: (process.env.DEFAULT_CURRENCY ?? "USD").toUpperCase(),
};
