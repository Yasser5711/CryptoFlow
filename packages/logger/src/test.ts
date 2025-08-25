import { createLogger, EMOJI } from "./index.js";

const logger = createLogger({
  name: "TEST",
  level: "debug",
  emoji: true,
});

console.log("\n🧪 Testing @cryptoflow/logger...\n");

logger.debug("Debug message");
logger.info("Info message");
logger.warn("Warning message");
logger.error("Error message");
logger.success("Success message");

logger.kafka("send", "Test Kafka message", { topic: "test.topic" });
logger.influx("write", "Test InfluxDB write", { points: 10 });
logger.price("BTC", 67234.5, "USDT");
logger.metric("test_metric", 42, "ms");

logger.box("TEST BOX", ["Line 1", "Line 2", "Line 3"]);
logger.separator();
logger.stats({ Test: "Value", Count: 123 });

console.log("\n✅ All tests passed!\n");
