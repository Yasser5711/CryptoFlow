import { publicProcedure, router } from "../lib/trpc";
import { publishEvent } from "../services/kafka";
import { queryRecent, queryCandles } from "../services/influx";
import { z } from "zod";
export const appRouter = router({
  healthCheck: publicProcedure
    .meta({
      openapi: {
        summary: "Health Check",
        description: "Check the health of the service",
        method: "GET",
        path: "/health",
      },
    })
    .input(z.void())
    .output(z.string())
    .query(() => {
      return "OK";
    }),
  publishMetric: publicProcedure
    .meta({
      openapi: {
        summary: "Publish a metric",
        description: "Publish a metric to the message broker",
        method: "POST",
        path: "/metrics",
      },
    })
    .input(
      z.object({
        measurement: z.string().min(1),
        tags: z.record(z.string(), z.string()).default({}),
        fields: z.record(z.string(), z.union([z.number(), z.string()])),
        ts: z.string().datetime().optional(),
      })
    )
    .output(z.object({ status: z.string() }))
    .mutation(async ({ input }) => {
      await publishEvent(input);
      return { status: "queued" };
    }),
  recentMetrics: publicProcedure
    .meta({
      openapi: {
        summary: "Get recent metrics",
        description: "Retrieve recent metrics from the database",
        method: "GET",
        path: "/metrics/recent",
      },
    })
    .input(
      z.object({
        measurement: z.string(),
        range: z.string().default("-15m"),
        limit: z.number().int().min(1).max(10_000).default(100),
        field: z
          .enum(["open", "high", "low", "close", "avg", "count"])
          .optional(),
        coin: z.string().optional(),
        currency: z.string().optional(),
        window: z.string().optional(),
        source: z.string().optional(),
      })
    )
    .output(z.array(z.record(z.string(), z.unknown())))
    .query(async ({ input }) => {
      const rows = await queryRecent({
        measurement: input.measurement,
        range: input.range,
        limit: input.limit,
        field: input.field,
        coin: input.coin,
        currency: input.currency,
        window: input.window,
        source: input.source,
      });
      console.log("Recent metrics:", rows);
      return rows;
    }),
  candles: publicProcedure
    .meta({
      openapi: {
        summary: "Candles (pivoted OHLC)",
        description: "Return pivoted OHLC candles from InfluxDB",
        method: "GET",
        path: "/candles",
      },
    })
    .input(
      z.object({
        measurement: z.string().default("price"),
        range: z.string().default("-6h"),
        limit: z.number().int().min(1).max(5000).default(2000),
        coin: z.string(),
        currency: z.enum(["USD", "EUR"]).optional(),
        window: z.enum(["1m", "5m", "15m", "1h"]).optional(),
        source: z.string().optional(),
      })
    )
    .output(
      z.array(
        z.object({
          _time: z.string(),
          coin: z.string(),
          currency: z.string(),
          window: z.string(),
          source: z.string().optional(),
          open: z.number(),
          high: z.number(),
          low: z.number(),
          close: z.number(),
        })
      )
    )
    .query(async ({ input }) => {
      const rows = await queryCandles(input);
      return rows as any;
    }),
});
export type AppRouter = typeof appRouter;
