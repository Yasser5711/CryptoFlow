const API_BASE = import.meta.env.VITE_API_URL;
const TIMEZONE = "Europe/Paris";
const MAX_LOG_ENTRIES = 100;

const $ = (id: string) => document.getElementById(id)!;
const elCoin = $("coin") as HTMLSelectElement;
const elWin = $("win") as HTMLSelectElement;
const elRange = $("range") as HTMLSelectElement;
const elCurrency = $("currency") as HTMLSelectElement;
const elPair = $("pair");
const elLast = $("lastPrice");
const elChange = $("change");
const elSrcInfo = $("srcInfo");
const elCandleInfo = $("candleInfo");
const elSSEStatus = $("sseStatus");
const elSSEText = $("sseText");
const elTickInfo = $("tickInfo");
const elCandleUpdates = $("candleUpdates");
const elLastUpdate = $("lastUpdate");
const elCandleCount = $("candleCount");
const elErr = $("err");
const elLogs = $("logs");

const chart = (window as any).echarts.init($("chart"));

let eventSource: EventSource | null = null;
let sseEnabled = true;
let logsVisible = false;

type HCandle = {
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
};
const historicalCandles = new Map<number, HCandle>();

let liveCandle: (HCandle & { count?: number; isLive: true }) | null = null;
const pendingFinalized = new Map<
  number,
  {
    timestamp: number;
    _time: string;
    open: number;
    high: number;
    low: number;
    close: number;
  }
>();

let tickCount = 0;
let candleUpdateCount = 0;
let latestLivePrice: { price: number; source: string; ts: string } | null =
  null;
const logEntries: Array<{
  timestamp: string;
  level: "INFO" | "WARN" | "ERROR";
  message: string;
  data?: any;
}> = [];

function log(
  level: "INFO" | "WARN" | "ERROR",
  message: string,
  data: any = null
) {
  const timestamp = new Date().toLocaleTimeString("fr-FR");
  const entry = { timestamp, level, message, data };
  logEntries.push(entry);
  if (logEntries.length > MAX_LOG_ENTRIES) logEntries.shift();
  if (logsVisible) renderLogs();

  if (level !== "INFO") {
    console.log(`[${level}] ${message}`, data || "");
  }
}

function renderLogs() {
  const html = logEntries
    .slice()
    .reverse()
    .map((entry) => {
      const dataStr = entry.data ? ` - ${JSON.stringify(entry.data)}` : "";
      return `
        <div class="log-entry">
          <span class="log-time">${entry.timestamp}</span>
          <span class="log-level ${entry.level}">${entry.level}</span>
          ${entry.message}${dataStr}
        </div>
      `;
    })
    .join("");
  elLogs.innerHTML = html;
}

function formatDateTime(dt: Date) {
  return new Intl.DateTimeFormat("fr-FR", {
    timeZone: TIMEZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).format(dt);
}

function formatPrice(price: number, currency: string) {
  try {
    return new Intl.NumberFormat("en-US", {
      style: "currency",
      currency,
      minimumFractionDigits: 2,
      maximumFractionDigits: 6,
    }).format(price);
  } catch {
    return `${price.toFixed(6)} ${currency}`;
  }
}

export function getWindowMs(window: string) {
  switch (window) {
    case "5m":
      return 300_000;
    case "15m":
      return 900_000;
    case "1h":
      return 3_600_000;
    case "1m":
    default:
      return 60_000;
  }
}
export function toCandleSeriesFromArray(
  arr: Array<{
    timestamp: number;
    open: number;
    close: number;
    low: number;
    high: number;
  }>
) {
  return arr.map((c) => [c.timestamp, c.open, c.close, c.low, c.high]);
}

export function reconcilePendingWithHistorical() {
  for (const ts of pendingFinalized.keys()) {
    if (historicalCandles.has(ts)) pendingFinalized.delete(ts);
  }
}

export function getDisplayBuckets() {
  const confirmed = Array.from(historicalCandles.values())
    .filter((c) => !pendingFinalized.has(c.timestamp))
    .sort((a, b) => a.timestamp - b.timestamp);

  const pending = Array.from(pendingFinalized.values()).sort(
    (a, b) => a.timestamp - b.timestamp
  );

  const live = liveCandle ? [liveCandle] : [];

  return { confirmed, pending, live };
}

function addHistoricalCandle(candle: any) {
  const timestamp = new Date(candle._time).getTime();
  historicalCandles.set(timestamp, {
    timestamp,
    _time: candle._time,
    open: candle.open,
    high: candle.high,
    low: candle.low,
    close: candle.close,
    coin: candle.coin,
    currency: candle.currency,
    window: candle.window,
    source: candle.source,
  });
}

function updateLiveCandle(candleData: {
  _time: string;
  open: number;
  high: number;
  low: number;
  close: number;
  count?: number;
}) {
  const incomingTs = new Date(candleData._time).getTime();

  if (liveCandle) {
    if (incomingTs > liveCandle.timestamp) {
      historicalCandles.set(liveCandle.timestamp, {
        timestamp: liveCandle.timestamp,
        _time: liveCandle._time,
        open: liveCandle.open,
        high: liveCandle.high,
        low: liveCandle.low,
        close: liveCandle.close,
        coin: liveCandle.coin,
        currency: liveCandle.currency,
        window: liveCandle.window,
        source: liveCandle.source,
      });
    }
  }

  liveCandle = {
    timestamp: incomingTs,
    _time: candleData._time,
    open: candleData.open,
    high: candleData.high,
    low: candleData.low,
    close: candleData.close,
    count: candleData.count,
    isLive: true,
  };

  candleUpdateCount++;
  elCandleUpdates.textContent = `Updates: ${candleUpdateCount}`;
}

function getCompletedCandles(): HCandle[] {
  return Array.from(historicalCandles.values()).sort(
    (a, b) => a.timestamp - b.timestamp
  );
}

function getAllCandlesForDisplay(): HCandle[] {
  const completed = getCompletedCandles();

  if (liveCandle) {
    const lastCompleted = completed[completed.length - 1];
    if (lastCompleted && lastCompleted.timestamp === liveCandle.timestamp) {
      return [...completed.slice(0, -1), liveCandle];
    }
    return [...completed, liveCandle];
  }
  return completed;
}

function connectSSE() {
  disconnectSSE();

  const coin = elCoin.value;
  const currency = elCurrency.value;
  const windowSel = elWin.value;
  const url = `${API_BASE}/api/stream/prices/${coin}?currency=${currency}&window=${windowSel}`;

  log("INFO", `SSE: Connecting to ${coin}/${currency}`);
  elSSEText.textContent = "SSE: Connecting...";
  elSSEStatus.className = "status-dot offline";

  eventSource = new EventSource(url);

  eventSource.addEventListener("connected", (e: MessageEvent) => {
    const data = JSON.parse(e.data);
    log("INFO", "SSE: Connected", { kafkaActive: data.kafkaActive });
    elSSEText.textContent = `SSE: Connected (${coin}/${currency})`;
    elSSEStatus.className = "status-dot online";
  });

  eventSource.addEventListener("tick", (e: MessageEvent) => {
    const data = JSON.parse(e.data);
    tickCount++;
    latestLivePrice = data;
    updateHeader(data);
    (elTickInfo as HTMLElement).textContent = `Ticks: ${tickCount}`;
    (elLastUpdate as HTMLElement).textContent = `Last: ${formatDateTime(
      new Date(data.ts)
    )}`;
  });

  eventSource.addEventListener("liveCandle", (e: MessageEvent) => {
    const data = JSON.parse(e.data);
    updateLiveCandle({
      _time: data.bucketStart,
      open: data.open,
      high: data.high,
      low: data.low,
      close: data.close,
      count: data.count,
    });
    renderCurrentChart();
  });

  eventSource.addEventListener("ping", () => {
    /* keep-alive */
  });

  eventSource.addEventListener("info", (e: MessageEvent) => {
    const data = JSON.parse(e.data);
    log("INFO", data.message);
  });

  eventSource.onerror = () => {
    log("WARN", "SSE: Connection error, reconnecting...");
    elSSEText.textContent = "SSE: Reconnecting...";
    elSSEStatus.className = "status-dot offline";
  };
}

function disconnectSSE() {
  if (eventSource) {
    eventSource.close();
    eventSource = null;
    log("INFO", "SSE: Disconnected");
    elSSEText.textContent = "SSE: Disconnected";
    elSSEStatus.className = "status-dot offline";
  }
}

function toCandleSeries(candles: HCandle[]) {
  return candles.map((c) => [c.timestamp, c.open, c.close, c.low, c.high]);
}

function SMA(series: any[], period: number) {
  const out: any[] = [];
  let sum = 0;
  const queue: number[] = [];

  for (let i = 0; i < series.length; i++) {
    const t = series[i][0] as number;
    const close = series[i][2] as number;
    queue.push(close);
    sum += close;

    if (queue.length > period) sum -= queue.shift()!;
    out.push([t, queue.length === period ? sum / period : null]);
  }
  return out;
}

function renderChart(
  candles: any[],
  ma7: any[],
  ma25: any[],
  liveMarker: { time: number; price: number } | null
) {
  if (!Array.isArray(candles) || candles.length === 0) {
    log("WARN", "No candle data to render");
    return;
  }

  const series: any[] = [
    {
      name: "OHLC",
      type: "candlestick",
      itemStyle: {
        color: "#16a69a",
        color0: "#dc2626",
        borderColor: "#16a69a",
        borderColor0: "#dc2626",
      },
      data: candles,
    },
    {
      name: "MA7",
      type: "line",
      showSymbol: false,
      smooth: true,
      lineStyle: { width: 1.5, color: "#3b82f6" },
      data: ma7,
    },
    {
      name: "MA25",
      type: "line",
      showSymbol: false,
      smooth: true,
      lineStyle: { width: 1.5, type: "dashed", color: "#8b5cf6" },
      data: ma25,
    },
  ];

  if (liveMarker) {
    series.push({
      name: "Live",
      type: "scatter",
      symbolSize: 8,
      itemStyle: { color: "#f59e0b" },
      data: [[liveMarker.time, liveMarker.price]],
      z: 10,
    });
  }

  const option = {
    animation: false,
    backgroundColor: "transparent",
    grid: { left: 60, right: 60, top: 40, bottom: 80 },
    tooltip: {
      trigger: "axis",
      axisPointer: { type: "cross" },
      backgroundColor: "rgba(50,50,50,0.9)",
      borderColor: "#333",
      textStyle: { color: "#fff" },
      valueFormatter: (v: any) =>
        typeof v === "number" ? formatPrice(v, elCurrency.value) : v,
    },
    xAxis: {
      type: "time",
      axisLabel: { formatter: (v: any) => formatDateTime(new Date(v)) },
    },
    yAxis: {
      type: "value",
      scale: true,
      axisLabel: { formatter: (v: any) => formatPrice(v, elCurrency.value) },
    },
    dataZoom: [{ type: "inside" }, { type: "slider", height: 30 }],
    toolbox: {
      feature: {
        saveAsImage: { title: "Save" },
        dataZoom: { yAxisIndex: "none" },
      },
    },
    series,
    legend: { top: 5, textStyle: { color: "inherit" } },
  };

  chart.setOption(option, true);
}

function renderCurrentChart() {
  const allCandles = getAllCandlesForDisplay();
  const candles = toCandleSeries(allCandles);
  const ma7 = SMA(candles, 7);
  const ma25 = SMA(candles, 25);

  const liveMarker = latestLivePrice
    ? { time: Date.now(), price: latestLivePrice.price }
    : null;

  renderChart(candles, ma7, ma25, liveMarker);

  const historicalCount = historicalCandles.size;
  const liveCount = liveCandle ? 1 : 0;
  elCandleCount.textContent = `Historical: ${historicalCount} | Live: ${liveCount}`;
}

async function fetchCandles() {
  const coin = elCoin.value;
  const windowSel = elWin.value;
  const range = elRange.value;
  const currency = elCurrency.value;

  const params = new URLSearchParams({ window: windowSel, range, currency });
  const url = `${API_BASE}/api/live-candles/${coin}?${params.toString()}`;

  log("INFO", `Fetching: ${coin}/${currency} ${windowSel} ${range}`);

  const response = await fetch(url);
  if (!response.ok)
    throw new Error(`HTTP ${response.status}: ${response.statusText}`);
  const data = await response.json();
  log("INFO", `Fetched ${data.candles.length} candles`);
  return data;
}

function updateHeader(priceData: {
  price: number;
  source: string;
  ts?: string;
}) {
  const coin = elCoin.value;
  const currency = elCurrency.value;
  elPair.textContent = `${coin}/${currency}`;
  elLast.textContent = formatPrice(priceData.price, currency);
  elSrcInfo.textContent = `source: ${priceData.source}`;
}

function computeChange(candles: HCandle[]) {
  if (!candles || candles.length < 2) return null;
  const first = candles[0].close;
  const last = candles[candles.length - 1].close;
  if (!Number.isFinite(first) || !Number.isFinite(last)) return null;
  const change = last - first;
  const changePct = (change / first) * 100;
  return { change, changePct, first, last };
}

async function refresh() {
  try {
    elErr.style.display = "none";

    const data = await fetchCandles();
    const { candles: rows, latest, liveCandle: serverLive } = data;

    historicalCandles.clear();
    liveCandle = null;

    rows.forEach((candle: any) => addHistoricalCandle(candle));

    if (serverLive) {
      updateLiveCandle({
        _time: serverLive.bucketStart,
        open: serverLive.open,
        high: serverLive.high,
        low: serverLive.low,
        close: serverLive.close,
        count: serverLive.count,
      });
      log("INFO", "Live candle loaded from server");
    }

    elCandleInfo.textContent = `candles: ${rows.length}`;

    const allCandles = getAllCandlesForDisplay();
    const change = computeChange(allCandles);
    if (change) {
      const arrow = change.changePct >= 0 ? "▲" : "▼";
      const sign = change.changePct >= 0 ? "+" : "";
      elChange.className = `chg ${change.changePct >= 0 ? "up" : "down"}`;
      elChange.textContent = `${arrow} ${sign}${change.changePct.toFixed(2)}%`;
    } else {
      elChange.className = "chg";
      elChange.textContent = "—";
    }

    if (latest) {
      updateHeader(latest);
      latestLivePrice = latest;
    } else if (allCandles.length > 0) {
      const lastCandle = allCandles[allCandles.length - 1];
      updateHeader({
        price: lastCandle.close,
        source: lastCandle.source || "unknown",
        ts: lastCandle._time,
      });
    }

    renderCurrentChart();
    log("INFO", "Chart refreshed successfully");
  } catch (error: any) {
    elErr.textContent = `Error: ${error.message}`;
    elErr.style.display = "block";
    log("ERROR", "Refresh failed", error.message);
  }
}

$("refresh").addEventListener("click", refresh);

$("toggleSSE").addEventListener("click", () => {
  sseEnabled = !sseEnabled;
  const btn = $("toggleSSE") as HTMLButtonElement;

  if (sseEnabled) {
    btn.textContent = "● Live ON";
    btn.className = "success";
    connectSSE();
  } else {
    btn.textContent = "○ Live OFF";
    btn.className = "danger";
    disconnectSSE();
  }
});

$("toggleLogs").addEventListener("click", () => {
  logsVisible = !logsVisible;
  elLogs.style.display = logsVisible ? "block" : "none";
  ($("toggleLogs") as HTMLButtonElement).textContent = logsVisible
    ? "Hide Logs"
    : "Show Logs";
  if (logsVisible) renderLogs();
});

elCoin.addEventListener("change", () => {
  tickCount = 0;
  candleUpdateCount = 0;
  liveCandle = null;
  historicalCandles.clear();
  refresh();
  if (sseEnabled) connectSSE();
});

elCurrency.addEventListener("change", () => {
  tickCount = 0;
  candleUpdateCount = 0;
  liveCandle = null;
  historicalCandles.clear();
  refresh();
  if (sseEnabled) connectSSE();
});

elWin.addEventListener("change", () => {
  liveCandle = null;
  refresh();
  if (sseEnabled) connectSSE();
});

elRange.addEventListener("change", refresh);
window.addEventListener("resize", () => chart.resize());

window.addEventListener("message", (event) => {
  if (event.data.type === "CONTROL") {
    const { coin, currency, range } = event.data.payload;

    if (coin) {
      elCoin.value = coin;
      elCoin.dispatchEvent(new Event("change"));
    }
    if (currency) {
      elCurrency.value = currency;
      elCurrency.dispatchEvent(new Event("change"));
    }
    if (range) {
      elRange.value = range;
      elRange.dispatchEvent(new Event("change"));
    }

    log("INFO", `IFrame Control: ${coin}/${currency} ${range}`);
  }
});

log("INFO", "Crypto Viewer initialized");
refresh();
if (sseEnabled) connectSSE();
