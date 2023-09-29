import Joi from 'joi';
import { PeerConfig } from './types';

export const pgSchema = Joi.object({
  host: Joi.string().required(),
  port: Joi.number().integer().min(1).max(65535).required(),
  database: Joi.string().min(1).max(100).required(),
  user: Joi.string().alphanum().min(1).max(64).required(),
  password: Joi.string().min(1).max(100).required(),
  transactionSnapshot: Joi.string().max(100).allow('').optional(),
});

export const checkFormFields = (peerType: string, config: PeerConfig) => {
  switch (peerType) {
    case 'POSTGRES':
      return pgSchema.validate(config);
    default:
      return { error: { message: 'Peer type not recognized' } };
  }
};
