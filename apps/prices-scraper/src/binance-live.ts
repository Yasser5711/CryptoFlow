import WebSocket from "ws";
import { env } from "./env";
import logger from "./logger";

export type BinanceTick = {
  source: "binance";
  symbol: string; // e.g. BTCUSDT
  baseAsset: string; // e.g. BTC
  quoteAsset: string; // e.g. USDT
  price: number;
  ts: string; // ISO UTC
};

class BinanceWebSocket {
  private ws: WebSocket | null = null;
  private connecting = false;
  private shouldReconnect = true;
  private reconnectAttempts = 0;
  private readonly maxReconnectAttempts = 10;
  private readonly baseDelayMs = 1000;
  private reconnectTimer: NodeJS.Timeout | null = null;

  private lastBySymbol = new Map<string, BinanceTick>();

  async connect(symbols: string[]): Promise<void> {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) return;
    if (this.connecting) return;

    this.connecting = true;

    try {
      const streams = symbols
        .map((s) => `${s.toLowerCase()}@miniTicker`)
        .join("/");
      const wsUrl = `${env.WS_URL}/stream?streams=${streams}`;

      logger.start(`Connecting to Binance WebSocket at ${wsUrl}`);
      this.ws = new WebSocket(wsUrl);

      this.ws.onopen = () => {
        this.connecting = false;
        this.reconnectAttempts = 0;
        if (this.reconnectTimer) {
          clearTimeout(this.reconnectTimer);
          this.reconnectTimer = null;
        }
        logger.ws("connect", "Binance WebSocket connected and live", {
          symbols,
        });
      };

      this.ws.onmessage = (event) => {
        try {
          const msg = JSON.parse(event.data as string);
          const tick = parseWsMessage(msg);
          if (!tick) return;
          this.lastBySymbol.set(tick.symbol, tick);
        } catch (e) {
          logger.warn("WS parse failed:", (e as Error).message);
        }
      };

      this.ws.onerror = (err) => {
        const errMsg = (err as unknown as Error)?.message ?? String(err);
        logger.ws("disconnect", `WebSocket error: ${errMsg}`);
      };

      this.ws.onclose = () => {
        this.connecting = false;
        logger.ws("disconnect", "WebSocket disconnected");
        this.scheduleReconnect(symbols);
      };

      setTimeout(() => {
        if (!this.ws) return;
        if (this.ws.readyState !== WebSocket.OPEN && this.connecting) {
          logger.ws("disconnect", "WebSocket connection timeout");
          try {
            this.ws.close();
          } catch {}
        }
      }, 10_000);
    } catch (e) {
      this.connecting = false;
      logger.error("Connection error:", e);
      this.scheduleReconnect(symbols);
    }
  }

  private scheduleReconnect(symbols: string[]) {
    if (!this.shouldReconnect) return;
    if (this.connecting) return;

    if (this.reconnectAttempts >= this.maxReconnectAttempts) {
      logger.error("Max reconnection attempts reached");
      return;
    }

    if (this.reconnectTimer) return;

    const delay = Math.min(
      this.baseDelayMs * Math.pow(2, this.reconnectAttempts),
      30_000
    );
    this.reconnectAttempts++;

    logger.info(
      `Reconnecting in ${delay}ms (attempt ${this.reconnectAttempts})`
    );
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      void this.connect(symbols);
    }, delay);
  }

  getMessages(): BinanceTick[] {
    const arr = Array.from(this.lastBySymbol.values());
    this.lastBySymbol.clear();
    return arr;
  }

  disconnect(): void {
    this.shouldReconnect = false;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    if (this.ws) {
      try {
        this.ws.close();
      } catch {}
      this.ws = null;
    }
  }

  isConnected(): boolean {
    return this.ws?.readyState === WebSocket.OPEN;
  }
}

const ws = new BinanceWebSocket();

function parseWsMessage(wsMessage: any): BinanceTick | null {
  try {
    const data = wsMessage?.data ?? wsMessage;
    if (!data) return null;

    if (data.e === "trade") {
      const symbol = data.s as string;
      const price = Number(data.p);
      if (!symbol || !Number.isFinite(price)) return null;
      const baseAsset = extractBaseAsset(symbol);
      if (!baseAsset) return null;
      const quoteAsset = symbol.slice(baseAsset.length);
      return {
        source: "binance",
        symbol,
        baseAsset,
        quoteAsset,
        price,
        ts: new Date(data.T).toISOString(),
      };
    }

    if (data.e === "24hrMiniTicker") {
      const symbol = data.s as string;
      const price = Number(data.c);
      if (!symbol || !Number.isFinite(price)) return null;
      const baseAsset = extractBaseAsset(symbol);
      if (!baseAsset) return null;
      const quoteAsset = symbol.slice(baseAsset.length);
      return {
        source: "binance",
        symbol,
        baseAsset,
        quoteAsset,
        price,
        ts: new Date(data.E).toISOString(),
      };
    }

    return null;
  } catch (e) {
    logger.warn("[binance] Failed to parse message:", e);
    return null;
  }
}

function extractBaseAsset(symbol: string): string | null {
  const quotes = ["USDT", "BUSD", "USDC", "USD", "EUR", "BTC", "ETH", "BNB"];
  for (const q of quotes) {
    if (symbol.endsWith(q)) return symbol.slice(0, symbol.length - q.length);
  }
  return null;
}

export async function fetchLiveTicks(): Promise<BinanceTick[]> {
  const symbols = env.PRICE_SYMBOLS;
  if (!symbols || symbols.length === 0) {
    logger.warn("[binance] No price symbols configured");
    return [];
  }

  if (!ws.isConnected()) {
    logger.info(
      "[binance] WebSocket not connected, connecting now...",
      symbols
    );
    await ws.connect(symbols);
  }

  const ticks = ws.getMessages();

  if (ticks.length) {
    logger.info(`[binance] ticks: ${ticks.length}`);

    for (const t of ticks.slice(0, 3)) {
      logger.price(t.symbol, t.price, t.quoteAsset);
    }
  }
  return ticks;
}

export async function shutdownBinanceWS(): Promise<void> {
  ws.disconnect();
}
