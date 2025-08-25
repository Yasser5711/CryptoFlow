import { el } from "./dom";
import { formatPrice, formatDateTime } from "./utils";
import { historicalCandles, getLiveCandle, getLatestLivePrice } from "./state";

export function updateHeader(priceData: {
  price: number;
  source: string;
  ts?: string;
}) {
  const coin = el.coin.value;
  const currency = el.currency.value;
  el.pair.textContent = `${coin}/${currency}`;
  el.last.textContent = formatPrice(priceData.price, currency);
  el.srcInfo.textContent = `source: ${priceData.source}`;
  if (priceData.ts) {
    el.lastUpdate.textContent = `Last: ${formatDateTime(
      new Date(priceData.ts)
    )}`;
  }
}

export function computeChange(candles: Array<{ close: number }>) {
  if (!candles || candles.length < 2) return null;
  const first = candles[0].close;
  const last = candles[candles.length - 1].close;
  if (!Number.isFinite(first) || !Number.isFinite(last)) return null;
  const change = last - first;
  const pct = (change / first) * 100;
  return { change, pct, first, last };
}

export function updateChangeBadge(candles: Array<{ close: number }>) {
  const res = computeChange(candles);
  if (!res) {
    el.change.className = "chg";
    el.change.textContent = "—";
    return;
  }
  const arrow = res.pct >= 0 ? "▲" : "▼";
  const sign = res.pct >= 0 ? "+" : "";
  el.change.className = `chg ${res.pct >= 0 ? "up" : "down"}`;
  el.change.textContent = `${arrow} ${sign}${res.pct.toFixed(2)}%`;
}

export function updateCounts() {
  const historicalCount = historicalCandles.size;
  const liveCount = getLiveCandle() ? 1 : 0;
  el.candleCount.textContent = `Historical: ${historicalCount} | Live: ${liveCount}`;
}

export function setSSEStatus(connected: boolean, text: string) {
  el.sseText.textContent = text;
  el.sseStatus.className = `status-dot ${connected ? "online" : "offline"}`;
}

export function updateTickCount(n: number) {
  el.tickInfo.textContent = `Ticks: ${n}`;
}
export function updateCandleUpdateCount(n: number) {
  el.candleUpdates.textContent = `Updates: ${n}`;
}

export function showError(msg: string) {
  el.err.textContent = `Error: ${msg}`;
  el.err.style.display = "block";
}
export function hideError() {
  el.err.style.display = "none";
}

export function headerFromLastCandleOrPrice(allCandles: any[]) {
  const latestLivePrice = getLatestLivePrice();
  if (latestLivePrice) return updateHeader(latestLivePrice);
  if (allCandles.length > 0) {
    const last = allCandles[allCandles.length - 1];
    updateHeader({
      price: last.close,
      source: last.source || "unknown",
      ts: last._time,
    });
  }
}
