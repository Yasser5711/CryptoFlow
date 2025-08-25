import { API_BASE } from "./constants";
import { el } from "./dom";
import { type Candle, historicalCandles, setLiveCandle } from "./state";
import { renderCurrentChart } from "./chart";
import { updateHeader, updateChangeBadge, hideError, showError } from "./ui";
import { log } from "./logger";

export async function fetchCandles() {
  const coin = el.coin.value;
  const window = el.win.value;
  const range = el.range.value;
  const currency = el.currency.value;

  const params = new URLSearchParams({ window, range, currency });
  const url = `${API_BASE}/api/live-candles/${coin}?${params.toString()}`;

  log("INFO", `Fetching: ${coin}/${currency} ${window} ${range}`);

  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`HTTP ${resp.status}: ${resp.statusText}`);
  return resp.json();
}

export function addHistoricalCandle(row: any) {
  const ts = new Date(row._time).getTime();
  const c: Candle = {
    timestamp: ts,
    _time: row._time,
    open: row.open,
    high: row.high,
    low: row.low,
    close: row.close,
    coin: row.coin,
    currency: row.currency,
    window: row.window,
    source: row.source,
  };
  historicalCandles.set(ts, c);
}

export async function refreshAll() {
  try {
    hideError();
    const data = await fetchCandles();
    const { candles: rows, latest, liveCandle: serverLiveCandle } = data;

    historicalCandles.clear();
    rows.forEach((row: any) => addHistoricalCandle(row));

    if (serverLiveCandle) {
      const ts = new Date(serverLiveCandle.bucketStart).getTime();
      setLiveCandle({
        timestamp: ts,
        _time: serverLiveCandle.bucketStart,
        open: serverLiveCandle.open,
        high: serverLiveCandle.high,
        low: serverLiveCandle.low,
        close: serverLiveCandle.close,
        count: serverLiveCandle.count,
      });
      log("INFO", "Live candle loaded from server");
    } else {
      setLiveCandle(null);
    }

    el.candleInfo.textContent = `candles: ${rows.length}`;

    const allCandles = Array.from(historicalCandles.values()).sort(
      (a, b) => a.timestamp - b.timestamp
    );
    updateChangeBadge(allCandles);

    if (latest) {
      updateHeader(latest);
    } else if (allCandles.length) {
      const last = allCandles[allCandles.length - 1];
      updateHeader({
        price: last.close,
        source: last.source || "unknown",
        ts: last._time,
      });
    }

    renderCurrentChart();
    log("INFO", "Chart refreshed successfully");
  } catch (e: any) {
    showError(e.message);
    log("ERROR", "Refresh failed", e.message);
  }
}
