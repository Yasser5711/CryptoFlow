export type Candle = {
  timestamp: number;
  _time: string;
  open: number;
  high: number;
  low: number;
  close: number;
  coin?: string;
  currency?: string;
  window?: string;
  source?: string;
  count?: number;
};

export let eventSource: EventSource | null = null;
export let sseEnabled = true;
export let logsVisible = false;

export const historicalCandles = new Map<number, Candle>();
let _liveCandle: Candle | null = null;

let _latestLivePrice: { price: number; source: string; ts: string } | null =
  null;
let _tickCount = 0;
let _candleUpdateCount = 0;

export function getLiveCandle() {
  return _liveCandle;
}
export function getLatestLivePrice() {
  return _latestLivePrice;
}
export function getTickCount() {
  return _tickCount;
}
export function getCandleUpdateCount() {
  return _candleUpdateCount;
}

export function setLiveCandle(c: Candle | null) {
  _liveCandle = c;
}
export function setLatestLivePrice(
  v: { price: number; source: string; ts: string } | null
) {
  _latestLivePrice = v;
}
export function incTickCount() {
  _tickCount += 1;
  return _tickCount;
}
export function incCandleUpdateCount() {
  _candleUpdateCount += 1;
  return _candleUpdateCount;
}
export function resetCounters() {
  _tickCount = 0;
  _candleUpdateCount = 0;
}

export function resetForPairChange() {
  resetCounters();
  setLiveCandle(null);
  setLatestLivePrice(null);
  historicalCandles.clear();
}
