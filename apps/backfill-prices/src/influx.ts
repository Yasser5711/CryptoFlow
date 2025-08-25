import { InfluxDB } from "@influxdata/influxdb-client";
import { env } from "./env";
const { INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET } = env;

if (!INFLUX_URL || !INFLUX_TOKEN || !INFLUX_ORG || !INFLUX_BUCKET) {
  throw new Error(
    "[backfill] Missing Influx env (INFLUX_URL/TOKEN/ORG/BUCKET)"
  );
}

const influx = new InfluxDB({ url: INFLUX_URL, token: INFLUX_TOKEN });
const queryApi = influx.getQueryApi(INFLUX_ORG);

/**
 * Retourne l’ensemble des timestamps (ms, fin de minute) déjà présents
 * pour (coin, currency, window=1m) et field='close' dans [startMs, endMs].
 */
export async function getPresentMinuteSet(params: {
  coin: string;
  currency: string;
  startMs: number;
  endMs: number;
}): Promise<Set<number>> {
  const { coin, currency, startMs, endMs } = params;

  const startIso = new Date(startMs).toISOString();
  const endIso = new Date(endMs).toISOString();

  const flux = `
from(bucket:"${INFLUX_BUCKET}")
  |> range(start: time(v: ${JSON.stringify(
    startIso
  )}), stop: time(v: ${JSON.stringify(endIso)}))
  |> filter(fn: (r) => 
      r._measurement == "price" and
      r.coin == ${JSON.stringify(coin)} and
      r.currency == ${JSON.stringify(currency)} and
      r.window == "1m" and
      r._field == "close"
  )
  |> keep(columns: ["_time"])
`;

  const tsSet = new Set<number>();
  return new Promise<Set<number>>((resolve, reject) => {
    queryApi.queryRows(flux, {
      next: (row, meta) => {
        const obj = meta.toObject(row) as { _time: string };
        const t = Date.parse(obj._time);
        if (Number.isFinite(t)) tsSet.add(t);
      },
      error: reject,
      complete: () => resolve(tsSet),
    });
  });
}
