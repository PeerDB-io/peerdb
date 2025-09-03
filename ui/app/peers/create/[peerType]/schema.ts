import { ehSchema } from '@/components/PeerForms/Eventhubs/schema';
import {
  AvroCodec,
  ElasticsearchAuthType,
  MySqlFlavor,
  MySqlReplicationMechanism,
} from '@/grpc_generated/peers';
import * as z from 'zod/v4';

const sshSchema = z
  .object({
    host: z
      .string({
        error: (issue) =>
          issue.input === undefined
            ? 'SSH Host is required'
            : 'SSH Host must be a string',
      })
      .min(1, { message: 'SSH Host cannot be empty' })
      .max(255, 'SSH Host must be less than 256 characters'),
    port: z
      .int({
        error: (issue) =>
          issue.input === undefined
            ? 'SSH Port is required'
            : 'SSH Port must be a number',
      })
      .min(1, 'SSH Port must be a positive integer')
      .max(65535, 'SSH Port must be below 65536'),
    user: z
      .string({
        error: (issue) =>
          issue.input === undefined
            ? 'SSH User is required'
            : 'SSH User must be a string',
      })
      .min(1, 'SSH User must be non-empty')
      .max(64, 'SSH User must be less than 65 characters'),
    password: z
      .string({
        error: (issue) =>
          issue.input === undefined
            ? 'SSH Password is required'
            : 'SSH Password must be a string',
      })
      .max(100, 'SSH Password must be less than 101 characters'),
    privateKey: z.string({
      error: (issue) =>
        issue.input === undefined
          ? 'SSH Private Key is required'
          : 'SSH Private Key must be a string',
    }),
  })
  .optional();

export const peerNameSchema = z
  .string({
    error: (issue) =>
      issue.input === undefined
        ? 'Peer name is invalid.'
        : 'Peer name is required.',
  })
  .min(1, { message: 'Peer name cannot be empty.' })
  .regex(/^[a-z_][a-z0-9_]*$/, {
    message:
      'Peer name must contain only lowercase letters, numbers and underscores',
  });

export const pgSchema = z.object({
  host: z
    .string({
      error: (issue) =>
        issue.input === undefined
          ? 'Host is required'
          : 'Host must be a string',
    })
    .min(1, { message: 'Host cannot be empty' })
    .max(255, 'Host must be less than 256 characters'),
  port: z
    .int({
      error: (issue) =>
        issue.input === undefined
          ? 'Port is required'
          : 'Port must be a number',
    })
    .min(1, 'Port must be a positive integer')
    .max(65535, 'Port must be below 65536'),
  database: z
    .string({
      error: (issue) =>
        issue.input === undefined
          ? 'Database is required'
          : 'Database must be a string',
    })
    .min(1, { message: 'Database name should be non-empty' })
    .max(100, 'Database must be less than 101 characters'),
  user: z
    .string({
      error: (issue) =>
        issue.input === undefined
          ? 'User is required'
          : 'User must be a string',
    })
    .min(1, 'User must be non-empty')
    .max(64, 'User must be less than 65 characters'),
  password: z
    .string({
      error: (issue) =>
        issue.input === undefined
          ? 'Password is required'
          : 'Password must be a string',
    })
    .max(100, 'Password must be less than 101 characters'),
  transactionSnapshot: z
    .string()
    .max(100, 'Transaction snapshot too long (100 char limit)')
    .optional(),
  requireTls: z.boolean(),
  rootCa: z
    .string({
      error: () => 'Root CA must be a string',
    })
    .optional()
    .transform((e) => (e === '' ? undefined : e)),
  tlsHost: z.string(),
  sshConfig: sshSchema,
});
export const mySchema = z.object({
  host: z
    .string({
      error: (issue) =>
        issue.input === undefined
          ? 'Host is required'
          : 'Host must be a string',
    })
    .min(1, { message: 'Host cannot be empty' })
    .max(255, 'Host must be less than 256 characters'),
  port: z
    .int({
      error: (issue) =>
        issue.input === undefined
          ? 'Port is required'
          : 'Port must be a number',
    })
    .min(1, 'Port must be a positive integer')
    .max(65535, 'Port must be below 65536'),
  user: z
    .string({
      error: (issue) =>
        issue.input === undefined
          ? 'User is required'
          : 'User must be a string',
    })
    .min(1, 'User must be non-empty')
    .max(64, 'User must be less than 65 characters'),
  password: z
    .string({
      error: (issue) =>
        issue.input === undefined
          ? 'Password is required'
          : 'Password must be a string',
    })
    .max(100, 'Password must be less than 101 characters'),
  compression: z.number().min(0).max(1),
  disableTls: z.boolean(),
  skipCertVerification: z.boolean(),
  flavor: z.union([
    z.literal(MySqlFlavor.MYSQL_MYSQL),
    z.literal(MySqlFlavor.MYSQL_MARIA),
  ]),
  replicationMechanism: z.union([
    z.literal(MySqlReplicationMechanism.MYSQL_AUTO),
    z.literal(MySqlReplicationMechanism.MYSQL_GTID),
    z.literal(MySqlReplicationMechanism.MYSQL_FILEPOS),
  ]),
  rootCa: z
    .string({
      error: () => 'Root CA must be a string',
    })
    .optional()
    .transform((e) => (e === '' ? undefined : e)),
  tlsHost: z.string(),
  sshConfig: sshSchema,
});

export const sfSchema = z.object({
  accountId: z
    .string({
      error: (issue) =>
        issue.input === undefined
          ? 'Account ID is required'
          : 'Account ID must be a string',
    })
    .min(1, { message: 'Account ID must be non-empty' })
    .max(255, 'Account ID must be less than 256 characters'),
  privateKey: z
    .string({
      error: (issue) =>
        issue.input === undefined
          ? 'Private Key is required'
          : 'Private Key must be a string',
    })
    .min(1, { message: 'Private Key must be non-empty' }),
  username: z
    .string({
      error: (issue) =>
        issue.input === undefined
          ? 'Username is required'
          : 'Username must be a string',
    })
    .min(1, { message: 'Username must be non-empty' })
    .max(255, 'Username must be less than 256 characters'),
  database: z
    .string({
      error: (issue) =>
        issue.input === undefined
          ? 'Database is required'
          : 'Database must be a string',
    })
    .min(1, { message: 'Database must be non-empty' })
    .max(255, 'Database must be less than 100 characters'),
  warehouse: z
    .string({
      error: (issue) =>
        issue.input === undefined
          ? 'Warehouse is required'
          : 'Warehouse must be a string',
    })
    .min(1, { message: 'Warehouse must be non-empty' })
    .max(255, 'Warehouse must be less than 64 characters'),
  role: z
    .string({
      error: (issue) =>
        issue.input === undefined
          ? 'Role is required'
          : 'Role must be a string',
    })
    .min(1, { message: 'Role must be non-empty' })
    .max(255, 'Role must be below 255 characters'),
  queryTimeout: z
    .int({
      error: () => 'Query timeout must be a number',
    })
    .min(0, 'Query timeout must be a positive integer')
    .max(65535, 'Query timeout must be below 65536 seconds')
    .optional(),
  password: z
    .string({
      error: () => 'Password must be a string',
    })
    .max(255, 'Password must be less than 255 characters')
    .optional()
    .transform((e) => (e === '' ? undefined : e)),
  s3Integration: z
    .string({
      error: () => 's3Integration must be a string',
    })
    .max(255, 's3Integration must be less than 255 characters')
    .optional(),
});

export const bqSchema = z.object({
  authType: z
    .string({
      error: (issue) =>
        issue.input === undefined
          ? 'Auth Type is required'
          : 'Auth Type must be a string',
    })
    .min(1, { message: 'Auth Type must be non-empty' })
    .max(255, 'Auth Type must be less than 256 characters'),
  projectId: z
    .string({
      error: (issue) =>
        issue.input === undefined
          ? 'Project ID is required'
          : 'Project ID must be a string',
    })
    .min(1, { message: 'Project ID must be non-empty' }),
  privateKeyId: z
    .string({
      error: (issue) =>
        issue.input === undefined
          ? 'Private Key ID is required'
          : 'Private Key ID must be a string',
    })
    .min(1, { message: 'Private Key must be non-empty' }),
  privateKey: z
    .string({
      error: (issue) =>
        issue.input === undefined
          ? 'Private Key is required'
          : 'Private Key must be a string',
    })
    .min(1, { message: 'Private Key must be non-empty' }),
  clientId: z
    .string({
      error: (issue) =>
        issue.input === undefined
          ? 'Client ID is required'
          : 'Client ID must be a string',
    })
    .min(1, { message: 'Client ID must be non-empty' }),
  clientEmail: z
    .string({
      error: (issue) =>
        issue.input === undefined
          ? 'Client Email is required'
          : 'Client Email must be a string',
    })
    .min(1, { message: 'Client Email must be non-empty' }),
  authUri: z
    .url({
      error: (issue) =>
        issue.input === undefined
          ? 'Auth URI is required'
          : 'Auth URI must be a URI',
    })
    .min(1, { message: 'Auth URI must be non-empty' }),
  tokenUri: z
    .url({
      error: (issue) =>
        issue.input === undefined
          ? 'Token URI is required'
          : 'Token URI must be a URI',
    })
    .min(1, { message: 'Token URI must be non-empty' }),
  authProviderX509CertUrl: z
    .url({
      error: (issue) =>
        issue.input === undefined
          ? 'Auth Cert URL is required'
          : 'Auth Cert URL must be a URI',
    })
    .min(1, { message: 'Auth Cert URL must be non-empty' }),
  clientX509CertUrl: z
    .url({
      error: (issue) =>
        issue.input === undefined
          ? 'Client Cert URL is required'
          : 'Client Cert URL must be a URI',
    })
    .min(1, { message: 'Client Cert URL must be non-empty' }),
  datasetId: z
    .string({
      error: (issue) =>
        issue.input === undefined
          ? 'Dataset ID is required'
          : 'Dataset ID must be a string',
    })
    .min(1, { message: 'Dataset ID must be non-empty' })
    .max(1024, 'DatasetID must be less than 1025 characters'),
  useAsSourceOnly: z.boolean().optional(),
});

export function chSchema(hostDomains: string[]) {
  return z.object({
    host: z
      .string({
        error: (issue) =>
          issue.input === undefined
            ? 'Host is required'
            : 'Host must be a string',
      })
      .min(1, { message: 'Host cannot be empty' })
      .max(255, 'Host must be less than 255 characters')
      .refine(
        (host) =>
          hostDomains.length === 0 ||
          hostDomains.some((domain) => host.endsWith(domain)),
        {
          message:
            'Host must end with one of the allowed domains: ' +
            hostDomains.join(', '),
        }
      ),
    port: z
      .int({
        error: (issue) =>
          issue.input === undefined
            ? 'Port is required'
            : 'Port must be a number',
      })
      .min(1, 'Port must be a positive integer')
      .max(65535, 'Port must be below 65536'),
    database: z
      .string({
        error: (issue) =>
          issue.input === undefined
            ? 'Database is required'
            : 'Database must be a string',
      })
      .min(1, { message: 'Database name should be non-empty' })
      .max(100, 'Database must be less than 100 characters'),
    user: z
      .string({
        error: (issue) =>
          issue.input === undefined
            ? 'User is required'
            : 'User must be a string',
      })
      .min(1, 'User must be non-empty')
      .max(128, 'User must be less than 128 characters'),
    password: z
      .string({
        error: () => 'Password must be a string',
      })
      .max(100, 'Password must be less than 100 characters')
      .optional(),
    s3Path: z.string({ error: () => 'S3 Path must be a string' }).optional(),
    accessKeyId: z
      .string({ error: () => 'Access Key ID must be a string' })
      .optional(),
    secretAccessKey: z
      .string({ error: () => 'Secret Access Key must be a string' })
      .optional(),
    region: z.string({ error: () => 'Region must be a string' }).optional(),
    endpoint: z.string({ error: () => 'Endpoint must be a string' }).optional(),
    disableTls: z.boolean(),
    certificate: z
      .string({
        error: () => 'Certificate must be a string',
      })
      .optional()
      .transform((e) => (e === '' ? undefined : e)),
    privateKey: z
      .string({
        error: () => 'Private Key must be a string',
      })
      .optional()
      .transform((e) => (e === '' ? undefined : e)),
    rootCa: z
      .string({
        error: () => 'Root CA must be a string',
      })
      .optional()
      .transform((e) => (e === '' ? undefined : e)),
    tlsHost: z.string(),
  });
}

export const kaSchema = z.object({
  servers: z
    .array(
      z.string({
        error: (issue) =>
          issue.input === undefined
            ? 'Server address must not be empty'
            : 'Invalid server provided',
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
      { error: () => ({ message: 'Invalid SASL mechanism' }) }
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
        error: () => ({ message: 'Invalid partitioning mechanism' }),
      }
    )
    .optional(),
  disableTls: z.boolean().optional(),
});

const urlSchema = z
  .string({
    error: (issue) =>
      issue.input === undefined ? 'URL is required' : 'URL must be a string',
  })
  .min(1, { message: 'URL must be non-empty' })
  .refine((url) => url.startsWith('s3://'), {
    message: 'URL must start with s3://',
  });

const accessKeySchema = z
  .string({
    error: (issue) =>
      issue.input === undefined
        ? 'Access Key ID is required'
        : 'Access Key ID must be a string',
  })
  .min(1, { message: 'Access Key ID must be non-empty' });

const secretKeySchema = z
  .string({
    error: (issue) =>
      issue.input === undefined
        ? 'Secret Access Key is required'
        : 'Secret Access Key must be a string',
  })
  .min(1, { message: 'Secret Access Key must be non-empty' });

const regionSchema = z.string({
  error: () => 'Region must be a string',
});

export const s3Schema = z.object({
  url: urlSchema,
  accessKeyId: accessKeySchema,
  secretAccessKey: secretKeySchema,
  roleArn: z
    .string({
      error: () => 'Role ARN must be a string',
    })
    .optional(),
  region: regionSchema.optional(),
  endpoint: z
    .string({
      error: () => 'Endpoint must be a string',
    })
    .optional(),
  codec: z.enum(AvroCodec, {
    error: (issue) =>
      issue.input === undefined
        ? 'Avro codec is required'
        : 'Avro codec must be one of [Null,Deflate,Snappy,ZStandard]',
  }),
});

export const psSchema = z.object({
  serviceAccount: z.object({
    authType: z
      .string({
        error: (issue) =>
          issue.input === undefined
            ? 'Auth Type is required'
            : 'Auth Type must be a string',
      })
      .min(1, { message: 'Auth Type must be non-empty' })
      .max(255, 'Auth Type must be less than 255 characters'),
    projectId: z
      .string({
        error: (issue) =>
          issue.input === undefined
            ? 'Project ID is required'
            : 'Project ID must be a string',
      })
      .min(1, { message: 'Project ID must be non-empty' }),
    privateKeyId: z
      .string({
        error: (issue) =>
          issue.input === undefined
            ? 'Private Key ID is required'
            : 'Private Key ID must be a string',
      })
      .min(1, { message: 'Private Key must be non-empty' }),
    privateKey: z
      .string({
        error: (issue) =>
          issue.input === undefined
            ? 'Private Key is required'
            : 'Private Key must be a string',
      })
      .min(1, { message: 'Private Key must be non-empty' }),
    clientId: z
      .string({
        error: (issue) =>
          issue.input === undefined
            ? 'Client ID is required'
            : 'Client ID must be a string',
      })
      .min(1, { message: 'Client ID must be non-empty' }),
    clientEmail: z
      .string({
        error: (issue) =>
          issue.input === undefined
            ? 'Client Email is required'
            : 'Client Email must be a string',
      })
      .min(1, { message: 'Client Email must be non-empty' }),
    authUri: z
      .url({
        error: (issue) =>
          issue.input === undefined
            ? 'Auth URI is required'
            : 'Auth URI must be a URI',
      })
      .min(1, { message: 'Auth URI must be non-empty' }),
    tokenUri: z
      .url({
        error: (issue) =>
          issue.input === undefined
            ? 'Token URI is required'
            : 'Token URI must be a URI',
      })
      .min(1, { message: 'Token URI must be non-empty' }),
    authProviderX509CertUrl: z
      .url({
        error: (issue) =>
          issue.input === undefined
            ? 'Auth Cert URL is required'
            : 'Auth Cert URL must be a URI',
      })
      .min(1, { message: 'Auth Cert URL must be non-empty' }),
    clientX509CertUrl: z
      .url({
        error: (issue) =>
          issue.input === undefined
            ? 'Client Cert URL is required'
            : 'Client Cert URL must be a URI',
      })
      .min(1, { message: 'Client Cert URL must be non-empty' }),
  }),
});

export const ehGroupSchema = z.object({
  // string to ehSchema map
  eventhubs: z
    .record(z.string(), ehSchema)
    .refine((obj) => Object.keys(obj).length > 0, {
      message: 'At least 1 Event Hub is required',
    }),
});

// slightly cursed, check for non-empty and non-whitespace string
function isString(i: string | undefined): boolean {
  return !!i && !!i.trim();
}

export const esSchema = z
  .object({
    addresses: z.array(
      z.url({
        message: 'Addresses must be a comma-seperated list of URLs',
      })
    ),
    authType: z.enum(ElasticsearchAuthType, {
      error: (issue) =>
        issue.input === undefined
          ? 'Auth type cannot be empty'
          : 'Auth type must be one of [none,basic,apikey]',
    }),
    username: z.string({ error: () => 'Username must be a string' }).optional(),
    password: z.string({ error: () => 'Password must be a string' }).optional(),
    apiKey: z.string({ error: () => 'API key must be a string' }).optional(),
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

export const mongoSchema = z.object({
  uri: z.string({ error: () => 'URI must be a string' }),
  username: z.string({ error: () => 'Username must be a string' }),
  password: z.string({ error: () => 'Password must be a string' }),
  disableTls: z.boolean().optional(),
  rootCa: z
    .string({ error: () => 'Root CA must be a string' })
    .optional()
    .transform((e) => (e === '' ? undefined : e)),
  tlsHost: z.string(),
});
