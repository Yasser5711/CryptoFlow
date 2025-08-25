import type { CreateFastifyContextOptions } from "@trpc/server/adapters/fastify";

export async function createContext({ req, res }: CreateFastifyContextOptions) {
  return {
    session: null,
  };
}

export type Context = Awaited<ReturnType<typeof createContext>>;
