# @cryptoflow/logger

Unified logger with Pino and emojis for CryptoFlow applications. Provides semantic logging methods with beautiful console output.

## 🎯 Features

- ✅ Built on top of Pino (fast & structured logging)
- 🎨 Colorized console output with Chalk
- 😊 Emoji support for better readability
- 📦 Semantic logging methods (kafka, influx, http, ws, etc.)
- 📊 Box/banner formatting for structured output
- 🚀 TypeScript support with full types
- ⚡ Performance-optimized

## 📦 Installation

```bash
yarn add @cryptoflow/logger
# or
npm install @cryptoflow/logger
```

## 🚀 Quick Start

```typescript
import { createLogger } from "@cryptoflow/logger";

const logger = createLogger({
  name: "my-app",
  level: "info",
  pretty: true,
  emoji: true,
});

logger.info("Application started");
logger.success("Connected to database");
logger.error("Failed to connect", { error: "Connection timeout" });
```

## 📖 API Reference

### Basic Logging

```typescript
logger.debug("Debug message", { details: "..." });
logger.info("Info message");
logger.warn("Warning message");
logger.error("Error message", { error: err });
logger.fatal("Fatal error");
logger.success("Operation completed successfully");
```

### Semantic Logging

#### Kafka Events

```typescript
logger.kafka("send", "Published message to topic", {
  topic: "prices.raw.v1",
  count: 5,
});
logger.kafka("receive", "Consumed message", { partition: 0 });
logger.kafka("connect", "Connected to Kafka broker");
```

#### InfluxDB Events

```typescript
logger.influx("write", "Written metrics to database", {
  measurement: "price",
  points: 10,
});
logger.influx("read", "Queried data", { query: "SELECT * FROM price" });
logger.influx("query", "Complex query executed");
```

#### HTTP Requests

```typescript
logger.http("GET", "/api/candles", 200, 45); // method, path, status, duration
```

#### WebSocket Events

```typescript
logger.ws("connect", "Client connected", { clientId: "abc123" });
logger.ws("disconnect", "Client disconnected");
logger.ws("message", "Received message");
```

#### Price Updates

```typescript
logger.price("BTC", 67234.5, "USD");
// Output: 💰 BTC → $67234.50 USD
```

#### Metrics

```typescript
logger.metric("latency", 42, "ms");
logger.metric("throughput", 1000, "req/s");
```

### Lifecycle Events

```typescript
logger.start("Starting application...");
logger.stop("Shutting down gracefully");
```

### Formatting

#### Box/Banner

```typescript
logger.box("CONFIGURATION", [
  "Port: 3000",
  "Environment: production",
  "Database: InfluxDB v2.7",
]);

// Output:
// ╔═══════════════════════════╗
// ║ CONFIGURATION             ║
// ╠═══════════════════════════╣
// ║ Port: 3000                ║
// ║ Environment: production   ║
// ║ Database: InfluxDB v2.7   ║
// ╚═══════════════════════════╝
```

#### Separator

```typescript
logger.separator(); // ────────────────────────────────
logger.separator("═", 50); // ══════════════════════════
```

#### Statistics

```typescript
logger.stats({
  "Total Requests": 1234,
  "Success Rate": "99.8%",
  "Avg Response": "42ms",
});
```

#### Progress Bar

```typescript
for (let i = 0; i <= 100; i++) {
  logger.progress(i, 100, "Processing");
  await sleep(10);
}
// Output: 🔄 Processing: [████████████░░░░░░░░] 60% (60/100)
```

## 🎨 Available Emojis

```typescript
import { EMOJI } from "@cryptoflow/logger";

// Level indicators
EMOJI.INFO; // ℹ️
EMOJI.WARN; // ⚠️
EMOJI.ERROR; // ❌
EMOJI.DEBUG; // 🔍
EMOJI.SUCCESS; // ✅

// Actions
EMOJI.START; // 🚀
EMOJI.STOP; // 🛑
EMOJI.SEND; // 📤
EMOJI.RECEIVE; // 📥
EMOJI.WRITE; // 💾
EMOJI.READ; // 📖

// Network
EMOJI.WEBSOCKET; // 🛰️
EMOJI.KAFKA; // 📦
EMOJI.DATABASE; // 💾
EMOJI.API; // 🔗

// Data
EMOJI.PRICE; // 💰
EMOJI.COIN; // 💎
EMOJI.METRICS; // 📈
EMOJI.STATS; // 📊

// ... and many more!
```

## 🔧 Configuration

```typescript
interface LoggerOptions {
  name: string; // Application name
  level?: LogLevel; // "debug" | "info" | "warn" | "error" | "fatal"
  pretty?: boolean; // Enable pretty printing (default: false)
  emoji?: boolean; // Enable emoji (default: true)
  timestamp?: boolean; // Show timestamps (default: true)
}
```

## 📝 Examples

### prices-scraper

```typescript
import { createLogger } from "@cryptoflow/logger";

const logger = createLogger({ name: "PRICES-SCRAPER" });

logger.start("Connecting to Binance WebSocket...");
logger.ws("connect", "Connected to Binance", { symbols: ["BTC", "ETH"] });

logger.price("BTC", 67234.5, "USDT");
logger.kafka("send", "Published to Kafka", {
  topic: "prices.raw.v1",
  count: 3,
});

logger.success("Scraper running!");
```

### analytics-prices

```typescript
const logger = createLogger({ name: "ANALYTICS-PRICES" });

logger.kafka("receive", "Consumed tick", { coin: "BTC", price: 67234.5 });
logger.info("Processing OHLC windows", { windows: ["1m", "5m", "15m", "1h"] });
logger.kafka("send", "Flushed windows", { count: 4 });

logger.stats({
  "Ticks Received": 1247,
  "Ticks Processed": 1247,
  "Success Rate": "100%",
});
```

### server

```typescript
const logger = createLogger({ name: "SERVER" });

logger.start("Starting server...");
logger.influx("write", "Persisted OHLC data", { measurement: "price" });
logger.http("GET", "/api/candles", 200, 42);
logger.ws("connect", "SSE client connected", { clientId: "abc123" });
logger.success("Server listening on port 3000");
```

## 🎯 Best Practices

1. **Use semantic methods** when available (kafka, influx, http, etc.)
2. **Include context data** in the second parameter
3. **Use appropriate log levels** (debug for verbose, info for normal, error for issues)
4. **Format complex data** with `logger.box()` or `logger.stats()`
5. **Track progress** with `logger.progress()` for long operations

## 🔄 Migration from Console.log

Before:

```typescript
console.log(`[KAFKA] Published ${count} messages to ${topic}`);
```

After:

```typescript
logger.kafka("send", "Published messages", { count, topic });
```

## 📊 Output Examples

```
2025-11-06T10:23:45.678Z [PRICES-SCRAPER] 🚀 Starting application...
2025-11-06T10:23:46.123Z [PRICES-SCRAPER] 🔌 [WEBSOCKET] Connected to Binance
   └─ { symbols: ["BTC", "ETH", "SOL"] }
2025-11-06T10:23:47.456Z [PRICES-SCRAPER] 💰 BTC → $67234.50 USDT
2025-11-06T10:23:47.789Z [PRICES-SCRAPER] 📤 [KAFKA] Published messages
   └─ { topic: "prices.raw.v1", count: 3 }
2025-11-06T10:23:48.012Z [PRICES-SCRAPER] ✅ Scraper running!
```

## 📚 Related Packages

- `@cryptoflow/banner` - ASCII banner utility
- `pino` - Fast logger for Node.js
- `chalk` - Terminal string styling

## 📄 License

MIT

## 👥 Authors

CryptoFlow Team
