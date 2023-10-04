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

export const sfSchema = z.object({
  accountId: z
    .string({
      required_error: 'Account ID is required',
      invalid_type_error: 'Account ID must be a string',
    })
    .nonempty({ message: 'Account ID must be non-empty' })
    .max(255, 'Account ID must be less than 255 characters'),
  privateKey: z
    .string({
      required_error: 'Private Key is required',
      invalid_type_error: 'Private Key must be a string',
    })
    .nonempty({ message: 'Private Key must be non-empty' }),
  username: z
    .string({
      required_error: 'Username is required',
      invalid_type_error: 'Username must be a string',
    })
    .nonempty({ message: 'Username must be non-empty' })
    .max(255, 'Username must be less than 255 characters'),
  database: z
    .string({
      required_error: 'Database is required',
      invalid_type_error: 'Database must be a string',
    })
    .nonempty({ message: 'Database must be non-empty' })
    .max(255, 'Database must be less than 100 characters'),
  warehouse: z
    .string({
      required_error: 'Warehouse is required',
      invalid_type_error: 'Warehouse must be a string',
    })
    .nonempty({ message: 'Warehouse must be non-empty' })
    .max(255, 'Warehouse must be less than 64 characters'),
  role: z
    .string({
      invalid_type_error: 'Role must be a string',
    })
    .nonempty({ message: 'Role must be non-empty' })
    .max(255, 'Role must be below 255 characters'),
  queryTimeout: z
    .number({
      invalid_type_error: 'Query timeout must be a number',
    })
    .int()
    .min(0, 'Query timeout must be a positive integer')
    .max(65535, 'Query timeout must be below 65535 seconds')
    .optional(),
  password: z
    .string({
      invalid_type_error: 'Password must be a string',
    })
    .max(255, 'Password must be less than 255 characters')
    .optional()
    .transform((e) => (e === '' ? undefined : e)),
  s3Integration: z
    .string({
      invalid_type_error: 's3Integration must be a string',
    })
    .max(255, 's3Integration must be less than 255 characters')
    .optional(),
});
