// @ts-ignore - ECharts global from CDN
declare const echarts: any;

import { el } from "./dom";
import {
  formatDateTime,
  formatPrice,
  SMA,
  toCandleSeriesFromArray,
} from "./utils";
import { historicalCandles, getLiveCandle } from "./state";
import { updateCounts } from "./ui";

const chart = echarts.init(el.chartContainer);

export function renderCurrentChart() {
  const confirmed = Array.from(historicalCandles.values()).sort(
    (a, b) => a.timestamp - b.timestamp
  );

  const seriesConfirmed = toCandleSeriesFromArray(
    confirmed.map((c) => ({
      timestamp: c.timestamp,
      open: c.open,
      close: c.close,
      low: c.low,
      high: c.high,
    }))
  );

  const ma7 = SMA(seriesConfirmed, 7);
  const ma25 = SMA(seriesConfirmed, 25);

  const live = getLiveCandle();
  const liveSeries = live
    ? [[live.timestamp, live.open, live.close, live.low, live.high]]
    : [];

  renderChart(seriesConfirmed, ma7, ma25, liveSeries);
  updateCounts();
}

function renderChart(
  confirmed: number[][],
  ma7: (number | null)[][],
  ma25: (number | null)[][],
  live: number[][]
) {
  if (!confirmed.length && !live.length) return;

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
      data: confirmed,
      z: 2,
    },
    {
      name: "MA7",
      type: "line",
      showSymbol: false,
      smooth: true,
      lineStyle: { width: 1.5, color: "#3b82f6" },
      data: ma7,
      z: 1,
    },
    {
      name: "MA25",
      type: "line",
      showSymbol: false,
      smooth: true,
      lineStyle: { width: 1.5, type: "dashed", color: "#8b5cf6" },
      data: ma25,
      z: 1,
    },
  ];

  if (live.length) {
    series.push({
      name: "LIVE",
      type: "candlestick",
      itemStyle: {
        color: "#f59e0b",
        color0: "#f59e0b",
        borderColor: "#f59e0b",
        borderColor0: "#f59e0b",
      },
      emphasis: { disabled: true },
      data: live,
      z: 3,
    });
  }

  const option = {
    animation: false,
    backgroundColor: "transparent",
    grid: { left: 10, right: 80, top: 40, bottom: 80 },
    tooltip: {
      trigger: "axis",
      axisPointer: { type: "cross" },
      backgroundColor: "rgba(50, 50, 50, 0.9)",
      borderColor: "#333",
      textStyle: { color: "#fff" },
      valueFormatter: (v: number) =>
        typeof v === "number"
          ? formatPrice(v, (el.currency as HTMLSelectElement).value)
          : v,
    },
    xAxis: {
      type: "time",
      axisLabel: { formatter: (v: number) => formatDateTime(new Date(v)) },
    },
    yAxis: {
      type: "value",
      scale: true,
      axisLabel: {
        formatter: (v: number) =>
          formatPrice(v, (el.currency as HTMLSelectElement).value),
      },
    },
    dataZoom: [{ type: "inside" }, { type: "slider", height: 30 }],
    toolbox: {
      feature: {
        saveAsImage: { title: "Save" },
        dataZoom: { yAxisIndex: "none" },
      },
    },
    legend: {
      top: 5,
      textStyle: { color: "inherit" },
      selected: { LIVE: false },
    },
    series,
  };

  chart.setOption(option, true);
}

export function resizeChart() {
  chart.resize();
}
