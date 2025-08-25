import { InfluxDB, Point, type QueryApi } from "@influxdata/influxdb-client";
import { env } from "../env";
import logger from "../lib/logger";

const { INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET } = env;

const FLUSH_MODE = env.INFLUX_FLUSH_MODE.toLowerCase() as
  | "immediate"
  | "interval";
const FLUSH_INTERVAL_MS = Number(env.INFLUX_FLUSH_INTERVAL_MS);

if (!INFLUX_URL || !INFLUX_TOKEN || !INFLUX_ORG || !INFLUX_BUCKET) {
  logger.warn("[influx] Missing configuration - some env vars not set");
}

const influx = new InfluxDB({ url: INFLUX_URL, token: INFLUX_TOKEN });
const writeApi = influx.getWriteApi(INFLUX_ORG, INFLUX_BUCKET, "ns");
const queryApi: QueryApi = influx.getQueryApi(INFLUX_ORG);

writeApi.useDefaultTags({ app: "crypto-metrics" });

let flushTimer: NodeJS.Timeout | null = null;

function scheduleFlush() {
  if (flushTimer) clearTimeout(flushTimer);
  flushTimer = setTimeout(() => {
    writeApi.flush().catch((e) => {
      logger.error("[influx] Flush error", { error: e?.message });
    });
  }, FLUSH_INTERVAL_MS);
}

const healthCheckInterval = setInterval(async () => {
  try {
    const testFlux = `
      from(bucket:"${INFLUX_BUCKET}")
      |> range(start: -1h)
      |> limit(n: 1)
    `;
    await new Promise<void>((resolve, reject) => {
      queryApi.queryRows(testFlux, {
        next: () => {},
        error: reject,
        complete: resolve,
      });
    });
    logger.debug("[influx] Health check passed");
  } catch (e: any) {
    logger.warn("[influx] Health check error", { error: e?.message });
  }
}, 30_000);

export async function writeMetric(
  measurement: string,
  tags: Record<string, string>,
  fields: Record<string, number | string>,
  ts?: Date
) {
  try {
    const p = new Point(measurement);
    Object.entries(tags).forEach(([k, v]) => p.tag(k, v));
    Object.entries(fields).forEach(([k, v]) => {
      if (typeof v === "number") p.floatField(k, v);
      else p.stringField(k, v);
    });
    if (ts) p.timestamp(ts);

    writeApi.writePoint(p);

    if (FLUSH_MODE === "immediate") {
      await writeApi.flush();
    } else {
      scheduleFlush();
    }
  } catch (e: any) {
    logger.error("[influx] Write error", { error: e?.message });
  }
}

function q(v?: string) {
  if (!v) return v;
  return v.replaceAll('"', '\\"');
}
function fluxRangeStart(range?: string): string {
  const r = (range ?? "-6h").trim();
  if (/^-\d+(m|h|d|w)$/i.test(r)) return r;
  return `time(v: "${q(r)}")`;
}

type QueryRecentParams = {
  measurement: string;
  range?: string;
  limit?: number;
  field?: "open" | "high" | "low" | "close" | "avg" | "count";
  coin?: string;
  currency?: string;
  window?: string;
  source?: string;
};

export async function queryRecent(p: QueryRecentParams) {
  const start = fluxRangeStart(p.range);
  const limit = Math.min(Math.max(p.limit ?? 100, 1), 10_000);

  const parts: string[] = [];
  parts.push(`from(bucket:"${INFLUX_BUCKET}")`);
  parts.push(`  |> range(start: ${start})`);
  parts.push(`  |> filter(fn: (r) => r._measurement == "${q(p.measurement)}")`);
  if (p.field) parts.push(`  |> filter(fn: (r) => r._field == "${p.field}")`);
  if (p.coin) parts.push(`  |> filter(fn: (r) => r.coin == "${q(p.coin)}")`);
  if (p.currency)
    parts.push(`  |> filter(fn: (r) => r.currency == "${q(p.currency)}")`);
  if (p.window)
    parts.push(`  |> filter(fn: (r) => r.window == "${q(p.window)}")`);
  if (p.source)
    parts.push(`  |> filter(fn: (r) => r.source == "${q(p.source)}")`);
  parts.push(`  |> sort(columns: ["_time"], desc: true)`);
  parts.push(`  |> limit(n: ${limit})`);

  const flux = parts.join("\n");
  const rows: Array<Record<string, unknown>> = [];
  return new Promise<typeof rows>((resolve, reject) => {
    queryApi.queryRows(flux, {
      next: (row, tableMeta) => {
        rows.push(tableMeta.toObject(row));
      },
      error: reject,
      complete: () => resolve(rows),
    });
  });
}

type QueryCandlesParams = {
  measurement: string;
  range?: string;
  limit?: number;
  coin: string;
  currency?: string;
  window?: "1m" | "5m" | "15m" | "1h";
  source?: string;
};

function rangeToMinutes(range?: string): number {
  const r = (range ?? "-6h").trim();
  const m = r.match(/^-(\d+)(m|h|d|w)$/i);
  if (!m) return 360;
  const n = Number(m[1]);
  const u = m[2].toLowerCase();
  if (u === "m") return n;
  if (u === "h") return n * 60;
  if (u === "d") return n * 1440;
  return n * 10080;
}
function winToMinutes(w: "1m" | "5m" | "15m" | "1h") {
  return w === "1m" ? 1 : w === "5m" ? 5 : w === "15m" ? 15 : 60;
}
const ALLOWED: Array<"1m" | "5m" | "15m" | "1h"> = ["1m", "5m", "15m", "1h"];

function pickWindowAuto(
  rangeMin: number,
  requested?: "1m" | "5m" | "15m" | "1h"
) {
  if (requested && ALLOWED.includes(requested)) {
    const cnt = Math.floor(rangeMin / winToMinutes(requested));
    if (cnt >= 30) return requested;
  }
  for (const w of ["1h", "15m", "5m", "1m"] as const) {
    const cnt = Math.floor(rangeMin / winToMinutes(w));
    if (cnt >= 60) return w;
  }
  return "1m";
}
function computeLimit(
  rangeMin: number,
  w: "1m" | "5m" | "15m" | "1h",
  override?: number
) {
  if (override && Number.isFinite(override)) return Math.min(override, 2500);
  const count = Math.max(1, Math.floor(rangeMin / winToMinutes(w)) + 2);
  return Math.min(count, 2500);
}

async function queryCandlesAggregated(p: QueryCandlesParams) {
  const start = fluxRangeStart(p.range);
  const rangeMin = rangeToMinutes(p.range);
  const every = pickWindowAuto(rangeMin, p.window);
  const limit = computeLimit(rangeMin, every, p.limit);

  const currencyFilter = p.currency
    ? ` and r.currency == "${q(p.currency)}"`
    : "";
  const sourceFilter = p.source ? ` and r.source == "${q(p.source)}"` : "";

  const dedupBlock = p.source
    ? ""
    : `
  |> map(fn: (r) => ({ r with srcRank: if r.source == "binance" then 2 else 1 }))
  |> group(columns: ["_time","coin","currency"])
  |> sort(columns: ["srcRank"], desc: true)
  |> limit(n: 1)
  |> group(columns: ["coin","currency"])
  |> drop(columns: ["srcRank"])
`;

  const base = `
base = from(bucket:"${INFLUX_BUCKET}")
  |> range(start: ${start})
  |> filter(fn: (r) => r._measurement == "${q(
    p.measurement
  )}" and r.coin == "${q(
    p.coin
  )}" and r.window == "1m"${currencyFilter}${sourceFilter})
  |> sort(columns: ["_time"], desc: false)
  ${dedupBlock}
`;

  const flux = `
${base}
opens = base |> filter(fn: (r) => r._field == "open")
             |> aggregateWindow(every: ${every}, fn: first, createEmpty: false)
             |> keep(columns: ["_time","coin","currency","_value"])
             |> rename(columns: {_value:"open"})

highs = base |> filter(fn: (r) => r._field == "high")
             |> aggregateWindow(every: ${every}, fn: max, createEmpty: false)
             |> keep(columns: ["_time","coin","currency","_value"])
             |> rename(columns: {_value:"high"})

lows  = base |> filter(fn: (r) => r._field == "low")
             |> aggregateWindow(every: ${every}, fn: min, createEmpty: false)
             |> keep(columns: ["_time","coin","currency","_value"])
             |> rename(columns: {_value:"low"})

closes= base |> filter(fn: (r) => r._field == "close")
             |> aggregateWindow(every: ${every}, fn: last, createEmpty: false)
             |> keep(columns: ["_time","coin","currency","_value"])
             |> rename(columns: {_value:"close"})

join1 = join(tables: {o: opens, h: highs}, on: ["_time","coin","currency"])
join2 = join(tables: {l: lows,  c: closes}, on: ["_time","coin","currency"])
join3 = join(tables: {a: join1, b: join2}, on: ["_time","coin","currency"])

join3
  |> keep(columns: ["_time","coin","currency","open","high","low","close"])
  |> set(key: "window", value: "${every}")
  |> sort(columns: ["_time"], desc: false)
  |> limit(n: ${limit})
`;

  const rows: Array<Record<string, unknown>> = [];
  return new Promise<typeof rows>((resolve, reject) => {
    queryApi.queryRows(flux, {
      next: (row, meta) => {
        rows.push(meta.toObject(row));
      },
      error: reject,
      complete: () => resolve(rows),
    });
  });
}

async function queryCandlesExact1m(p: QueryCandlesParams) {
  const start = fluxRangeStart(p.range);
  const limit = Math.min(p.limit ?? 2000, 5000);
  const currencyFilter = p.currency
    ? ` and r.currency == "${q(p.currency)}"`
    : "";
  const sourceFilter = p.source ? ` and r.source == "${q(p.source)}"` : "";

  const flux = `
from(bucket:"${INFLUX_BUCKET}")
  |> range(start: ${start})
  |> filter(fn: (r) => r._measurement == "${q(
    p.measurement
  )}" and r.coin == "${q(
    p.coin
  )}" and r.window == "1m"${currencyFilter}${sourceFilter})
  |> filter(fn: (r) => r._field == "open" or r._field == "high" or r._field == "low" or r._field == "close")
  |> sort(columns: ["_time"], desc: false)
  |> pivot(rowKey: ["_time","coin","currency","window","source"], columnKey: ["_field"], valueColumn: "_value")
  |> keep(columns: ["_time","coin","currency","window","source","open","high","low","close"])
  |> limit(n: ${limit})
`;
  const rows: Array<Record<string, unknown>> = [];
  return new Promise<typeof rows>((resolve, reject) => {
    queryApi.queryRows(flux, {
      next: (row, meta) => {
        rows.push(meta.toObject(row));
      },
      error: reject,
      complete: () => resolve(rows),
    });
  });
}

export async function queryCandles(p: QueryCandlesParams) {
  let rows = await queryCandlesAggregated(p);
  if (!rows.length) rows = await queryCandlesExact1m(p);
  return rows;
}

export async function closeInflux() {
  try {
    clearInterval(healthCheckInterval);
    if (flushTimer) clearTimeout(flushTimer);
    await writeApi.close();
  } catch (e) {
    logger.warn("Error closing InfluxDB client", {
      error: (e as Error).message,
    });
  }
}
