export const env = {
  KAFKA_BROKERS: process.env
    .KAFKA_BROKERS!.split(",")
    .map((s) => s.trim())
    .filter(Boolean),
  PRICES_TOPIC: process.env.PRICES_TOPIC!,
  PRICE_SYMBOLS: process.env
    .PRICE_SYMBOLS!.split(",")
    .map((s) => s.trim().toUpperCase())
    .filter(Boolean),
  WS_URL: process.env.WS_URL!,

  POLL_INTERVAL_MS: Number(process.env.POLL_INTERVAL_MS || "1000"),
};
