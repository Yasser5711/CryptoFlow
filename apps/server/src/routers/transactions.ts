import { router, publicProcedure } from '../lib/trpc';

const transactionsData = [
  { id: '1', type: 'buy', coin: 'Solana', amount: '+5.00 SOL', time: '01:45 PM', icon: '◉' },
  { id: '2', type: 'sell', coin: 'USDC', amount: '-2.70 USDC', time: '01:32 PM', icon: '◉' },
  { id: '3', type: 'buy', coin: 'Cardano', amount: '-1.02 ADA', time: '00:30 AM', icon: '₳' },
  { id: '4', type: 'buy', coin: 'Shiba Inu', amount: '+2.31 SHIB', time: '11:24 AM', icon: '🐕' }
];

export const transactionsRouter = router({
  getTransactions: publicProcedure.query(() => {
    return transactionsData;
  }),
});
