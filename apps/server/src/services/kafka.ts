import {
  Kafka,
  type LogEntry,
  logLevel,
  type Consumer,
  type Producer,
} from "kafkajs";
import { writeMetric } from "./influx";
import { env } from "../env";
import logger from "../lib/logger";

const { KAFKA_BROKERS, KAFKA_CLIENT_ID, KAFKA_TOPIC, KAFKA_GROUP_ID } = env;

let kafka: Kafka | null = null;
let producer: Producer | null = null;
let consumer: Consumer | null = null;
let ready = false;
let isConnecting = false;

function getKafkaInstance() {
  if (!kafka) {
    const brokers = KAFKA_BROKERS.split(",")
      .map((b) => b.trim())
      .filter(Boolean);

    if (!brokers.length) {
      throw new Error("KAFKA_BROKERS is not configured");
    }

    kafka = new Kafka({
      clientId: KAFKA_CLIENT_ID,
      brokers,
      logCreator: () => (entry: LogEntry) => {
        const { level, log } = entry;
        const msg = `[KafkaJS] ${log.message}`;
        if (level === logLevel.ERROR) {
          logger.error(msg, log);
        }
      },
      retry: {
        initialRetryTime: 300,
        retries: 15,
        multiplier: 2,
        maxRetryTime: 30000,
      },
      connectionTimeout: 10000,
      requestTimeout: 30000,
      ssl: false,
    });
  }
  return kafka;
}

function fmt(n: unknown) {
  return typeof n === "number" ? n.toFixed(2) : String(n);
}

export async function startKafka() {
  if (isConnecting) {
    throw new Error("Kafka connection already in progress");
  }
  if (ready && producer && consumer) {
    logger.kafka("connect", "Kafka already started");
    return;
  }

  isConnecting = true;
  try {
    const kafkaInstance = getKafkaInstance();

    producer = kafkaInstance.producer({
      idempotent: true,
      maxInFlightRequests: 1,
      retry: { initialRetryTime: 300, retries: 8, multiplier: 2 },
    });

    logger.kafka("connect", "Connecting producer…");
    await producer.connect();
    logger.success("✓ Producer connected");

    consumer = kafkaInstance.consumer({
      groupId: KAFKA_GROUP_ID,
      sessionTimeout: 20000,
      rebalanceTimeout: 60000,
    });

    logger.kafka("connect", "Connecting consumer…", {
      groupId: KAFKA_GROUP_ID,
    });
    await consumer.connect();
    logger.success("✓ Consumer connected");

    logger.kafka("connect", `Subscribing to topic: ${KAFKA_TOPIC}`);
    await consumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: false });

    await consumer.run({
      eachMessage: async ({ message, partition }) => {
        if (!message.value) return;

        try {
          const evt = JSON.parse(message.value.toString());

          logger.influx("write", "Writing metric to InfluxDB", {
            measurement: evt.measurement,
            tags: evt.tags,
            fields: {
              open: fmt(evt.fields?.open),
              high: fmt(evt.fields?.high),
              low: fmt(evt.fields?.low),
              close: fmt(evt.fields?.close),
            },
            ts: evt.ts || "now",
            partition,
          });

          const startWrite = Date.now();
          const fieldsRaw = evt.fields ?? {};

          const normalized: Record<string, number> = {
            open: Number(fieldsRaw.open ?? fieldsRaw.O),
            high: Number(fieldsRaw.high ?? fieldsRaw.H),
            low: Number(fieldsRaw.low ?? fieldsRaw.L),
            close: Number(fieldsRaw.close ?? fieldsRaw.C),
          };

          for (const k of ["open", "high", "low", "close"] as const) {
            if (!Number.isFinite(normalized[k])) {
              throw new Error(
                `Invalid ${k} field (got: ${
                  fieldsRaw[k] ?? fieldsRaw[k?.toUpperCase()]
                })`
              );
            }
          }

          const tags = {
            ...(evt.tags ?? {}),
            window: evt.tags?.window ?? "1m",
            coin: evt.tags?.coin,
            currency: evt.tags?.currency?.toUpperCase(),
            source: evt.tags?.source,
          };
          if (!tags.coin || !tags.currency) {
            throw new Error("Missing tags.coin or tags.currency");
          }

          const canonicalCurrency =
            tags.currency === "USDT" ? "USD" : tags.currency;
          const writes: Array<{
            currency: string;
            fields: Record<string, number>;
          }> = [{ currency: canonicalCurrency, fields: normalized }];

          const fx = Number(process.env.FX_USD_TO_EUR || "0");
          if (canonicalCurrency === "USD" && Number.isFinite(fx) && fx > 0) {
            writes.push({
              currency: "EUR",
              fields: {
                open: normalized.open * fx,
                high: normalized.high * fx,
                low: normalized.low * fx,
                close: normalized.close * fx,
              },
            });
          }

          for (const write of writes) {
            await writeMetric(
              evt.measurement ?? "price",
              { ...tags, currency: write.currency },
              write.fields,
              evt.ts ? new Date(evt.ts) : undefined
            );
          }
          const writeDuration = Date.now() - startWrite;

          logger.success("✓ Influx write OK", {
            writeDurationMs: writeDuration,
          });
        } catch (e: any) {
          logger.error(`✗ parse/write error on ${KAFKA_TOPIC}`, {
            error: e?.message,
            valuePreview: message.value.toString().slice(0, 200),
          });
        }
      },
    });

    ready = true;
    logger.success("Kafka started and consuming messages");
  } catch (e: any) {
    logger.error("Failed to start Kafka", { error: e?.message });
    await stopKafka();
    throw e;
  } finally {
    isConnecting = false;
  }
}

export async function publishEvent(evt: {
  measurement: string;
  tags?: Record<string, string>;
  fields: Record<string, number | string>;
  ts?: string;
}) {
  if (!producer) {
    throw new Error("Kafka producer not initialized");
  }

  try {
    logger.kafka("send", `Publishing event to ${KAFKA_TOPIC}`, {
      measurement: evt.measurement,
      tags: evt.tags,

      fieldsKeys: Object.keys(evt.fields || {}),
      hasTs: !!evt.ts,
    });

    await producer.send({
      topic: KAFKA_TOPIC,
      messages: [{ value: JSON.stringify(evt) }],
    });

    logger.success("✓ Event published");
  } catch (e: any) {
    logger.error("Failed to publish event", { error: e?.message });
    throw e;
  }
}

export async function stopKafka() {
  ready = false;

  try {
    if (consumer) {
      await consumer.disconnect();
      logger.kafka("connect", "Consumer disconnected");
    }
  } catch (e) {
    logger.error("Error disconnecting consumer", {
      error: (e as Error).message,
    });
  }

  try {
    if (producer) {
      await producer.disconnect();
      logger.kafka("connect", "Producer disconnected");
    }
  } catch (e) {
    logger.error("Error disconnecting producer", {
      error: (e as Error).message,
    });
  }

  producer = null;
  consumer = null;
  kafka = null;
}

export async function isKafkaReady() {
  return ready;
}

export function getConsumer() {
  return consumer;
}
