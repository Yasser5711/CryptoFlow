import { initTRPC, TRPCError } from "@trpc/server";
import type { Context } from "./context";
import SuperJSON from "superjson";
import type { OpenApiMeta } from "trpc-to-openapi";
export const t = initTRPC.meta<OpenApiMeta>().context<Context>().create({
  transformer: SuperJSON,
});

export const router = t.router;

export const publicProcedure = t.procedure;
