<p align="center">
  <img src="https://img.shields.io/badge/CryptoFlow-🌊-blue?style=for-the-badge" alt="CryptoFlow" />
</p>

<h1 align="center">🌊 CryptoFlow</h1>

<p align="center">
  <strong>Real-time cryptocurrency data pipeline with live price analytics</strong>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/TypeScript-007ACC?style=flat-square&logo=typescript&logoColor=white" alt="TypeScript" />
  <img src="https://img.shields.io/badge/Kafka-231F20?style=flat-square&logo=apache-kafka&logoColor=white" alt="Kafka" />
  <img src="https://img.shields.io/badge/InfluxDB-22ADF6?style=flat-square&logo=influxdb&logoColor=white" alt="InfluxDB" />
  <img src="https://img.shields.io/badge/Node.js-339933?style=flat-square&logo=node.js&logoColor=white" alt="Node.js" />
  <img src="https://img.shields.io/badge/Docker-2496ED?style=flat-square&logo=docker&logoColor=white" alt="Docker" />
  <img src="https://img.shields.io/badge/Turborepo-EF4444?style=flat-square&logo=turborepo&logoColor=white" alt="Turborepo" />
</p>

<p align="center">
  <a href="#-architecture">Architecture</a> •
  <a href="#-features">Features</a> •
  <a href="#-quick-start">Quick Start</a> •
  <a href="#-apps">Apps</a> •
  <a href="#-packages">Packages</a>
</p>

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              CryptoFlow Pipeline                            │
└─────────────────────────────────────────────────────────────────────────────┘

  ┌──────────────┐      ┌──────────────┐      ┌──────────────┐      ┌──────────────┐
  │   Binance    │      │    Kafka     │      │   InfluxDB   │      │    Viewer    │
  │  WebSocket   │ ───▶ │   Streams    │ ───▶ │  Time-Series │ ───▶ │   Frontend   │
  │    (Live)    │      │              │      │   Database   │      │  (ECharts)   │
  └──────────────┘      └──────────────┘      └──────────────┘      └──────────────┘
         │                     │                     │                     │
         │                     │                     │                     │
         ▼                     ▼                     ▼                     ▼
  ┌──────────────┐      ┌──────────────┐      ┌──────────────┐      ┌──────────────┐
  │   prices-    │      │  analytics-  │      │    server    │      │     SSE      │
  │   scraper    │ ───▶ │    prices    │ ───▶ │   (tRPC)     │ ───▶ │  Real-Time   │
  │              │      │   (OHLC)     │      │              │      │   Updates    │
  └──────────────┘      └──────────────┘      └──────────────┘      └──────────────┘


    📡 Producer          🔄 Processor          💾 Storage           📊 Display
    Raw price ticks      Aggregates to         Persists OHLC        Live candlestick
    from exchanges       1m, 5m, 15m, 1h       candles              charts
```

### Data Flow

1. **🔌 Data Ingestion** — `prices-scraper` connects to Binance WebSocket and streams raw price ticks to Kafka (`prices.raw.v1`)

2. **⚙️ Processing** — `analytics-prices` consumes ticks, aggregates them into OHLC candles (1m, 5m, 15m, 1h windows), and publishes to Kafka (`metrics.events`)

3. **💾 Storage** — `server` consumes processed candles and persists them to InfluxDB time-series database

4. **📊 Visualization** — `viewer` fetches historical data via tRPC API and receives real-time updates via SSE

---

## ✨ Features

| Feature               | Description                                           |
| --------------------- | ----------------------------------------------------- |
| 🚀 **Real-Time**      | Sub-second price updates via WebSocket & SSE          |
| 📈 **OHLC Candles**   | Multiple timeframes: 1m, 5m, 15m, 1h                  |
| 🪙 **Multi-Coin**     | BTC, ETH, SOL, BNB, ADA, XRP, DOGE, LINK, MATIC, AVAX |
| 💹 **Live Charts**    | Interactive candlestick charts with ECharts           |
| 🔄 **Backfill**       | Historical data import from Binance API               |
| 🐳 **Dockerized**     | Full Docker Compose stack ready to deploy             |
| 📦 **Monorepo**       | Turborepo-powered for fast builds                     |
| 🎨 **Beautiful Logs** | Semantic logging with emojis                          |

---

## 🚀 Quick Start

### Prerequisites

- **Node.js** ≥ 20
- **Yarn** ≥ 4.x (Berry)
- **Docker** & **Docker Compose**

### 1️⃣ Clone & Install

```bash
git clone https://github.com/Yasser5711/CryptoFlow
cd CryptoFlow
yarn install
```

### 2️⃣ Start Infrastructure

```bash
# Start Kafka, Zookeeper, and InfluxDB
docker compose up -d
```

### 3️⃣ Configure Environment

```bash
cp .env.example .env
# Edit .env with your settings
```

```env
# Kafka
KAFKA_BROKERS=localhost:29092

# InfluxDB
INFLUX_URL=http://localhost:8086
INFLUX_TOKEN=dev-token
INFLUX_ORG=demo-org
INFLUX_BUCKET=demo-bucket

# Server
PORT=3000
CORS_ORIGIN=http://localhost:5173
```

### 4️⃣ Run Services

```bash
# Development mode (all services)
yarn dev

# Or run individually
yarn dev:prices-scraper    # Start price ingestion
yarn dev:analytics-prices  # Start OHLC aggregation
yarn dev:server           # Start API server
yarn dev:viewer           # Start frontend
```

### 5️⃣ Open the Viewer

Navigate to `http://localhost:5173` to see live candlestick charts! 📊

---

## 📁 Apps

### 🔌 prices-scraper

> Real-time price ingestion from Binance WebSocket

- Connects to Binance WebSocket streams
- Publishes raw price ticks to Kafka `prices.raw.v1`
- Health check endpoint at `/healthz`

```bash
yarn dev:prices-scraper
```

### ⚙️ analytics-prices

> OHLC candle aggregation engine

- Consumes raw ticks from Kafka
- Aggregates into 1m, 5m, 15m, 1h OHLC windows
- Publishes candles to Kafka `metrics.events`

```bash
yarn dev:analytics-prices
```

### 🖥️ server

> tRPC API server with real-time SSE

- RESTful & tRPC endpoints
- InfluxDB persistence layer
- SSE streaming for live updates
- OpenAPI documentation via Scalar

```bash
yarn dev:server
# API: http://localhost:3000
# Docs: http://localhost:3000/docs
```

### 📊 viewer

> Real-time candlestick visualization

- Interactive ECharts candlestick charts
- Real-time SSE price updates
- Multi-coin & multi-timeframe support
- Dark/Light theme

```bash
yarn dev:viewer
# UI: http://localhost:5173
```

### 📥 backfill-prices

> Historical data backfill utility

- Fetches historical klines from Binance API
- Backfills missing data to InfluxDB
- Smart deduplication

```bash
yarn dev:backfill-prices
```

---

## 📦 Packages

### @cryptoflow/logger

> Unified logging with Pino + emojis

```typescript
import { createLogger } from "@cryptoflow/logger";

const logger = createLogger({ name: "MY-APP" });

logger.info("Starting...");
logger.kafka("send", "Published message", { topic: "prices.raw.v1" });
logger.influx("write", "Persisted data", { points: 10 });
logger.success("Ready!");
```

### @cryptoflow/banner

> ASCII art banner utility

```typescript
import { printBanner } from "@cryptoflow/banner";

printBanner("SERVER", "Listening on http://localhost:3000");
```

---

## 🐳 Docker

### Full Stack

```bash
# Start everything
docker compose up -d

# View logs
docker compose logs -f

# Stop all
docker compose down
```

### Services

| Service     | Port  | Description             |
| ----------- | ----- | ----------------------- |
| `kafka`     | 29092 | Kafka broker            |
| `zookeeper` | 22181 | Zookeeper               |
| `influxdb`  | 8086  | InfluxDB time-series DB |

---

## 🛠️ Development

### Project Structure

```
cryptoflow/
├── apps/
│   ├── analytics-prices/   # OHLC aggregation
│   ├── backfill-prices/    # Historical backfill
│   ├── prices-scraper/     # Binance WebSocket
│   ├── server/             # tRPC API server
│   └── viewer/             # Frontend UI
├── packages/
│   ├── banner/             # ASCII banner
│   └── logger/             # Unified logger
├── docker-compose.yml
├── turbo.json
└── package.json
```

### Scripts

| Command            | Description                    |
| ------------------ | ------------------------------ |
| `yarn dev`         | Start all services in dev mode |
| `yarn build`       | Build all packages             |
| `yarn check-types` | TypeScript type checking       |

### Tech Stack

- **Runtime**: Node.js + TypeScript
- **Build**: Turborepo + TSC
- **Message Queue**: Apache Kafka
- **Database**: InfluxDB 2.7
- **API**: Fastify + tRPC
- **Frontend**: Vite + ECharts
- **Containerization**: Docker Compose

---

## 📈 Monitoring

### InfluxDB UI

Access at `http://localhost:8086`

- **Username**: admin
- **Password**: adminadmin
- **Organization**: demo-org
- **Bucket**: demo-bucket

### Health Checks

```bash
# Prices Scraper
curl http://localhost:3101/healthz

# Analytics Prices
curl http://localhost:3102/healthz

# Server
curl http://localhost:3000/health
```

---

## 🤝 Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'feat: add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## 📄 License

MIT © CryptoFlow Team
