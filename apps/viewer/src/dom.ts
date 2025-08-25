export const $ = (id: string) => document.getElementById(id)!;

export const el = {
  coin: $("coin") as HTMLSelectElement,
  win: $("win") as HTMLSelectElement,
  range: $("range") as HTMLSelectElement,
  currency: $("currency") as HTMLSelectElement,

  pair: $("pair"),
  last: $("lastPrice"),
  change: $("change"),
  srcInfo: $("srcInfo"),
  candleInfo: $("candleInfo"),

  sseStatus: $("sseStatus"),
  sseText: $("sseText"),
  tickInfo: $("tickInfo"),
  candleUpdates: $("candleUpdates"),
  lastUpdate: $("lastUpdate"),
  candleCount: $("candleCount"),

  err: $("err"),
  logs: $("logs"),
  chartContainer: $("chart"),
  refreshBtn: $("refresh"),
  toggleSSEBtn: $("toggleSSE"),
  toggleLogsBtn: $("toggleLogs"),
};
