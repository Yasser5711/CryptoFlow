import { API_BASE } from "./constants";
import { el } from "./dom";
import { formatDateTime } from "./utils";
import { renderCurrentChart } from "./chart";
import { setLatestLivePrice, incTickCount, setLiveCandle } from "./state";
import {
  updateHeader,
  setSSEStatus,
  updateTickCount,
  updateCandleUpdateCount,
} from "./ui";
import { log } from "./logger";

let es: EventSource | null = null;

export function disconnectSSE() {
  if (es) {
    es.close();
    es = null;
  }
  log("INFO", "SSE: Disconnected");
  setSSEStatus(false, "SSE: Disconnected");
}

export function connectSSE() {
  disconnectSSE();

  const coin = el.coin.value;
  const currency = el.currency.value;
  const windowVal = el.win.value;
  const url = `${API_BASE}/api/stream/prices/${coin}?currency=${currency}&window=${windowVal}`;

  log("INFO", `SSE: Connecting to ${coin}/${currency}`);
  setSSEStatus(false, "SSE: Connecting...");

  es = new EventSource(url);

  es.addEventListener("connected", (e: MessageEvent) => {
    const data = JSON.parse(e.data);
    log("INFO", "SSE: Connected", { kafkaActive: data.kafkaActive });
    setSSEStatus(true, `SSE: Connected (${coin}/${currency})`);
  });

  es.addEventListener("tick", (e: MessageEvent) => {
    const data = JSON.parse(e.data);
    setLatestLivePrice(data);
    const n = incTickCount();
    updateHeader(data);
    updateTickCount(n);
    el.lastUpdate.textContent = `Last: ${formatDateTime(new Date(data.ts))}`;
  });

  es.addEventListener("liveCandle", (e: MessageEvent) => {
    const d = JSON.parse(e.data);
    const ts = new Date(d.bucketStart).getTime();
    setLiveCandle({
      timestamp: ts,
      _time: d.bucketStart,
      open: d.open,
      high: d.high,
      low: d.low,
      close: d.close,
      count: d.count,
    });
    updateCandleUpdateCount(
      // @ts-ignore: on le stocke côté UI via une fermeture
      (window.__candleUpdates = (window.__candleUpdates || 0) + 1)
    );
    renderCurrentChart();
  });

  es.addEventListener("ping", () => {});
  es.addEventListener("info", (e: MessageEvent) =>
    log("INFO", JSON.parse(e.data).message)
  );

  es.onerror = () => {
    log("WARN", "SSE: Connection error, reconnecting...");
    setSSEStatus(false, "SSE: Reconnecting...");
  };
}

export function isSSEEnabled() {
  return true;
}
