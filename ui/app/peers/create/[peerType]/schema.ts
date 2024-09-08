import { ehSchema } from '@/components/PeerForms/Eventhubs/schema';
import { ElasticsearchAuthType } from '@/grpc_generated/peers';
import * as z from 'zod';

export const peerNameSchema = z
  .string({
    invalid_type_error: 'Peer name is invalid.',
    required_error: 'Peer name is required.',
  })
  .min(1, { message: 'Peer name cannot be empty.' })
  .regex(/^[a-z_][a-z0-9_]*$/, {
    message:
      'Peer name must contain only lowercase letters, numbers and underscores',
  });

export const pgSchema = z.object({
  host: z
    .string({
      required_error: 'Host is required',
      invalid_type_error: 'Host must be a string',
    })
    .min(1, { message: 'Host cannot be empty' })
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
  sshConfig: z
    .object({
      host: z
        .string({
          required_error: 'SSH Host is required',
          invalid_type_error: 'SSH Host must be a string',
        })
        .min(1, { message: 'SSH Host cannot be empty' })
        .max(255, 'SSH Host must be less than 255 characters'),
      port: z
        .number({
          required_error: 'SSH Port is required',
          invalid_type_error: 'SSH Port must be a number',
        })
        .int()
        .min(1, 'SSH Port must be a positive integer')
        .max(65535, 'SSH Port must be below 65535'),
      user: z
        .string({
          required_error: 'SSH User is required',
          invalid_type_error: 'SSH User must be a string',
        })
        .min(1, 'SSH User must be non-empty')
        .max(64, 'SSH User must be less than 64 characters'),
      password: z
        .string({
          required_error: 'SSH Password is required',
          invalid_type_error: 'SSH Password must be a string',
        })
        .max(100, 'SSH Password must be less than 100 characters'),
      privateKey: z.string({
        required_error: 'SSH Private Key is required',
        invalid_type_error: 'SSH Private Key must be a string',
      }),
    })
    .optional(),
});

export const sfSchema = z.object({
  accountId: z
    .string({
      required_error: 'Account ID is required',
      invalid_type_error: 'Account ID must be a string',
    })
    .min(1, { message: 'Account ID must be non-empty' })
    .max(255, 'Account ID must be less than 255 characters'),
  privateKey: z
    .string({
      required_error: 'Private Key is required',
      invalid_type_error: 'Private Key must be a string',
    })
    .min(1, { message: 'Private Key must be non-empty' }),
  username: z
    .string({
      required_error: 'Username is required',
      invalid_type_error: 'Username must be a string',
    })
    .min(1, { message: 'Username must be non-empty' })
    .max(255, 'Username must be less than 255 characters'),
  database: z
    .string({
      required_error: 'Database is required',
      invalid_type_error: 'Database must be a string',
    })
    .min(1, { message: 'Database must be non-empty' })
    .max(255, 'Database must be less than 100 characters'),
  warehouse: z
    .string({
      required_error: 'Warehouse is required',
      invalid_type_error: 'Warehouse must be a string',
    })
    .min(1, { message: 'Warehouse must be non-empty' })
    .max(255, 'Warehouse must be less than 64 characters'),
  role: z
    .string({
      invalid_type_error: 'Role must be a string',
    })
    .min(1, { message: 'Role must be non-empty' })
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

export const bqSchema = z.object({
  authType: z
    .string({
      required_error: 'Auth Type is required',
      invalid_type_error: 'Auth Type must be a string',
    })
    .min(1, { message: 'Auth Type must be non-empty' })
    .max(255, 'Auth Type must be less than 255 characters'),
  projectId: z
    .string({
      required_error: 'Project ID is required',
      invalid_type_error: 'Project ID must be a string',
    })
    .min(1, { message: 'Project ID must be non-empty' }),
  privateKeyId: z
    .string({
      required_error: 'Private Key ID is required',
      invalid_type_error: 'Private Key ID must be a string',
    })
    .min(1, { message: 'Private Key must be non-empty' }),
  privateKey: z
    .string({
      required_error: 'Private Key is required',
      invalid_type_error: 'Private Key must be a string',
    })
    .min(1, { message: 'Private Key must be non-empty' }),
  clientId: z
    .string({
      required_error: 'Client ID is required',
      invalid_type_error: 'Client ID must be a string',
    })
    .min(1, { message: 'Client ID must be non-empty' }),
  clientEmail: z
    .string({
      required_error: 'Client Email is required',
      invalid_type_error: 'Client Email must be a string',
    })
    .min(1, { message: 'Client Email must be non-empty' }),
  authUri: z
    .string({
      required_error: 'Auth URI is required',
      invalid_type_error: 'Auth URI must be a string',
    })
    .url({ message: 'Invalid auth URI' })
    .min(1, { message: 'Auth URI must be non-empty' }),
  tokenUri: z
    .string({
      required_error: 'Token URI is required',
      invalid_type_error: 'Token URI must be a string',
    })
    .url({ message: 'Invalid token URI' })
    .min(1, { message: 'Token URI must be non-empty' }),
  authProviderX509CertUrl: z
    .string({
      invalid_type_error: 'Auth Cert URL must be a string',
      required_error: 'Auth Cert URL is required',
    })
    .url({ message: 'Invalid auth cert URL' })
    .min(1, { message: 'Auth Cert URL must be non-empty' }),
  clientX509CertUrl: z
    .string({
      invalid_type_error: 'Client Cert URL must be a string',
      required_error: 'Client Cert URL is required',
    })
    .url({ message: 'Invalid client cert URL' })
    .min(1, { message: 'Client Cert URL must be non-empty' }),
  datasetId: z
    .string({
      invalid_type_error: 'Dataset ID must be a string',
      required_error: 'Dataset ID is required',
    })
    .min(1, { message: 'Dataset ID must be non-empty' })
    .max(1024, 'DatasetID must be less than 1025 characters'),
});

export const chSchema = (hostDomains: string[]) =>
  z.object({
    host: z
      .string({
        required_error: 'Host is required',
        invalid_type_error: 'Host must be a string',
      })
      .min(1, { message: 'Host cannot be empty' })
      .max(255, 'Host must be less than 255 characters')
      .refine(
        (host) => {
          if (hostDomains.length > 0) {
            return hostDomains.some((domain) => host.endsWith(domain));
          }
          return true;
        },
        {
          message:
            'Host must end with one of the allowed domains: ' +
            hostDomains.join(', '),
        }
      ),
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
      .max(256, 'User must be less than 64 characters'),
    password: z
      .string({
        invalid_type_error: 'Password must be a string',
      })
      .max(100, 'Password must be less than 100 characters')
      .optional(),
    s3Path: z
      .string({ invalid_type_error: 'S3 Path must be a string' })
      .optional(),
    accessKeyId: z
      .string({ invalid_type_error: 'Access Key ID must be a string' })
      .optional(),
    secretAccessKey: z
      .string({ invalid_type_error: 'Secret Access Key must be a string' })
      .optional(),
    region: z
      .string({ invalid_type_error: 'Region must be a string' })
      .optional(),
    endpoint: z
      .string({ invalid_type_error: 'Endpoint must be a string' })
      .optional(),
    disableTls: z.boolean(),
    certificate: z
      .string({
        invalid_type_error: 'Certificate must be a string',
      })
      .optional()
      .transform((e) => (e === '' ? undefined : e)),
    privateKey: z
      .string({
        invalid_type_error: 'Private Key must be a string',
      })
      .optional()
      .transform((e) => (e === '' ? undefined : e)),
    rootCa: z
      .string({
        invalid_type_error: 'Root CA must be a string',
      })
      .optional()
      .transform((e) => (e === '' ? undefined : e)),
  });

export const kaSchema = z.object({
  servers: z
    .array(
      z.string({
        invalid_type_error: 'Invalid server provided',
        required_error: 'Server address must not be empty',
      })
    )
    .min(1, { message: 'At least 1 server required' }),
  username: z.string().optional(),
  password: z.string().optional(),
  sasl: z
    .union(
      [
        z.literal('PLAIN'),
        z.literal('SCRAM-SHA-256'),
        z.literal('SCRAM-SHA-512'),
      ],
      { errorMap: (issue, ctx) => ({ message: 'Invalid SASL mechanism' }) }
    )
    .optional(),
  partitioner: z
    .union(
      [
        z.literal('Default'),
        z.literal('LeastBackup'),
        z.literal('Manual'),
        z.literal('RoundRobin'),
        z.literal('StickyKey'),
        z.literal('Sticky'),
        z.literal(''),
      ],
      {
        errorMap: (issue, ctx) => ({
          message: 'Invalid partitioning mechanism',
        }),
      }
    )
    .optional(),
  disableTls: z.boolean().optional(),
});

const urlSchema = z
  .string({
    invalid_type_error: 'URL must be a string',
    required_error: 'URL is required',
  })
  .min(1, { message: 'URL must be non-empty' })
  .refine((url) => url.startsWith('s3://'), {
    message: 'URL must start with s3://',
  });

const accessKeySchema = z
  .string({
    invalid_type_error: 'Access Key ID must be a string',
    required_error: 'Access Key ID is required',
  })
  .min(1, { message: 'Access Key ID must be non-empty' });

const secretKeySchema = z
  .string({
    invalid_type_error: 'Secret Access Key must be a string',
    required_error: 'Secret Access Key is required',
  })
  .min(1, { message: 'Secret Access Key must be non-empty' });

const regionSchema = z.string({
  invalid_type_error: 'Region must be a string',
});

export const s3Schema = z.object({
  url: urlSchema,
  accessKeyId: accessKeySchema,
  secretAccessKey: secretKeySchema,
  roleArn: z
    .string({
      invalid_type_error: 'Role ARN must be a string',
    })
    .optional(),
  region: regionSchema.optional(),
  endpoint: z
    .string({
      invalid_type_error: 'Endpoint must be a string',
    })
    .optional(),
});

export const psSchema = z.object({
  serviceAccount: z.object({
    authType: z
      .string({
        required_error: 'Auth Type is required',
        invalid_type_error: 'Auth Type must be a string',
      })
      .min(1, { message: 'Auth Type must be non-empty' })
      .max(255, 'Auth Type must be less than 255 characters'),
    projectId: z
      .string({
        required_error: 'Project ID is required',
        invalid_type_error: 'Project ID must be a string',
      })
      .min(1, { message: 'Project ID must be non-empty' }),
    privateKeyId: z
      .string({
        required_error: 'Private Key ID is required',
        invalid_type_error: 'Private Key ID must be a string',
      })
      .min(1, { message: 'Private Key must be non-empty' }),
    privateKey: z
      .string({
        required_error: 'Private Key is required',
        invalid_type_error: 'Private Key must be a string',
      })
      .min(1, { message: 'Private Key must be non-empty' }),
    clientId: z
      .string({
        required_error: 'Client ID is required',
        invalid_type_error: 'Client ID must be a string',
      })
      .min(1, { message: 'Client ID must be non-empty' }),
    clientEmail: z
      .string({
        required_error: 'Client Email is required',
        invalid_type_error: 'Client Email must be a string',
      })
      .min(1, { message: 'Client Email must be non-empty' }),
    authUri: z
      .string({
        required_error: 'Auth URI is required',
        invalid_type_error: 'Auth URI must be a string',
      })
      .url({ message: 'Invalid auth URI' })
      .min(1, { message: 'Auth URI must be non-empty' }),
    tokenUri: z
      .string({
        required_error: 'Token URI is required',
        invalid_type_error: 'Token URI must be a string',
      })
      .url({ message: 'Invalid token URI' })
      .min(1, { message: 'Token URI must be non-empty' }),
    authProviderX509CertUrl: z
      .string({
        invalid_type_error: 'Auth Cert URL must be a string',
        required_error: 'Auth Cert URL is required',
      })
      .url({ message: 'Invalid auth cert URL' })
      .min(1, { message: 'Auth Cert URL must be non-empty' }),
    clientX509CertUrl: z
      .string({
        invalid_type_error: 'Client Cert URL must be a string',
        required_error: 'Client Cert URL is required',
      })
      .url({ message: 'Invalid client cert URL' })
      .min(1, { message: 'Client Cert URL must be non-empty' }),
  }),
});

export const ehGroupSchema = z.object({
  // string to ehSchema map
  eventhubs: z.record(ehSchema).refine((obj) => Object.keys(obj).length > 0, {
    message: 'At least 1 Event Hub is required',
  }),
});

// slightly cursed, check for non-empty and non-whitespace string
const isString = (i: string | undefined): boolean => {
  return !!i && !!i.trim();
};

export const esSchema = z
  .object({
    addresses: z.array(
      z.string().url({
        message: 'Addresses must be a comma-seperated list of URLs',
      })
    ),
    authType: z.nativeEnum(ElasticsearchAuthType, {
      required_error: 'Auth type cannot be empty',
      invalid_type_error: 'Auth type must be one of [none,basic,apikey]',
    }),
    username: z
      .string({
        invalid_type_error: 'Username must be a string',
      })
      .optional(),
    password: z
      .string({
        invalid_type_error: 'Password must be a string',
      })
      .optional(),
    apiKey: z
      .string({
        invalid_type_error: 'API key must be a string',
      })
      .optional(),
  })
  .refine(
    (esSchema) => {
      if (esSchema.authType === ElasticsearchAuthType.BASIC) {
        return (
          isString(esSchema.username) &&
          isString(esSchema.password) &&
          !isString(esSchema.apiKey)
        );
      } else if (esSchema.authType === ElasticsearchAuthType.APIKEY) {
        return (
          !isString(esSchema.username) &&
          !isString(esSchema.password) &&
          isString(esSchema.apiKey)
        );
      } else if (esSchema.authType === ElasticsearchAuthType.NONE) {
        return (
          !isString(esSchema.username) &&
          !isString(esSchema.password) &&
          !isString(esSchema.apiKey)
        );
      }
    },
    {
      message: 'Authentication info not valid',
    }
  );
