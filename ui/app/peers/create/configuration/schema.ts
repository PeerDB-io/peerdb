import * as z from 'zod';
import { PeerConfig } from './types';

const pgSchema = z.object({
  host: z.string().nonempty().max(255),
  port: z.number().int().min(1).max(65535),
  database: z.string().min(1).max(100),
  user: z.string().min(1).max(64),
  password: z.string().min(1).max(100),
  transactionSnapshot: z.string().max(100).optional(),
});

export const checkFormFields = (peerType: string, config: PeerConfig) => {
  switch (peerType) {
    case 'POSTGRES':
      return pgSchema.safeParse(config);
    default:
      return { success: false, error: { message: 'Peer type not recognized' } };
  }
};
