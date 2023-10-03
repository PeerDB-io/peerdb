import * as z from 'zod';

export const pgSchema = z.object({
  host: z
    .string({
      required_error: 'Host is required',
      invalid_type_error: 'Host must be a string',
    })
    .nonempty()
    .max(255, 'Host must be less than 255 characters'),
  port: z
    .number({
      required_error: 'Port is required',
      invalid_type_error: 'Port must be a number',
    })
    .int()
    .min(1, 'Port must be a positive integer')
    .max(65535, 'Port must be below 65535'),
  database: z
    .string({
      required_error: 'Database is required',
      invalid_type_error: 'Database must be a string',
    })
    .min(1, { message: 'Database name should be non-empty' })
    .max(100, 'Database must be less than 100 characters'),
  user: z
    .string({
      required_error: 'User is required',
      invalid_type_error: 'User must be a string',
    })
    .min(1, 'User must be non-empty')
    .max(64, 'User must be less than 64 characters'),
  password: z
    .string({
      required_error: 'Password is required',
      invalid_type_error: 'Password must be a string',
    })
    .min(1, 'Password must be non-empty')
    .max(100, 'Password must be less than 100 characters'),
  transactionSnapshot: z
    .string()
    .max(100, 'Transaction snapshot too long (100 char limit)')
    .optional(),
});
