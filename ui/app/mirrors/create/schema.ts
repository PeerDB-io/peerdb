import * as z from 'zod';

export const flowNameSchema = z
  .string({
    invalid_type_error: 'Mirror name is invalid.',
    required_error: 'Mirror name is required.',
  })
  .min(1, { message: 'Mirror name cannot be empty.' })
  .regex(/^[a-z_][a-z0-9_]*$/, {
    message:
      'Mirror name must contain only lowercase letters, numbers and underscores',
  });

export const tableMappingSchema = z
  .array(
    z.object({
      sourceTableIdentifier: z
        .string()
        .min(1, 'source table names, if added, must be non-empty'),
      destinationTableIdentifier: z
        .string()
        .min(1, 'destination table names, if added, must be non-empty'),
      exclude: z.array(z.string()).optional(),
      partitionKey: z.string().optional(),
    })
  )
  .nonempty('At least one table mapping is required')
  .superRefine((mappingArray, ctx) => {
    if (
      mappingArray.map((val) => val.destinationTableIdentifier).length !==
      new Set(mappingArray).size
    ) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: `Two source tables have been mapped to the same destination table`,
      });
    }
  });

export const cdcSchema = z.object({
  sourceName: z.string({ required_error: 'Source peer is required' }).min(1),
  destinationName: z
    .string({ required_error: 'Destination peer is required' })
    .min(1),
  doInitialCopy: z.boolean().optional(),
  publicationName: z
    .string({
      invalid_type_error: 'Publication name must be a string',
    })
    .max(255, 'Publication name must be less than 256 characters')
    .optional(),
  replicationSlotName: z
    .string({
      invalid_type_error: 'Replication slot name must be a string',
    })
    .max(255, 'Replication slot name must be less than 256 characters')
    .optional(),
  snapshotNumRowsPerPartition: z
    .number({
      invalid_type_error: 'Snapshot rows per partition must be a number',
    })
    .int()
    .min(1, 'Snapshot rows per partition must be a positive integer')
    .optional(),
  snapshotMaxParallelWorkers: z
    .number({
      invalid_type_error: 'Initial load parallelism must be a number',
    })
    .int()
    .min(1, 'Initial load parallelism must be a positive integer')
    .optional(),
  snapshotNumTablesInParallel: z
    .number({
      invalid_type_error: 'Snapshot parallel tables must be a number',
    })
    .int()
    .min(1, 'Snapshot parallel tables must be a positive integer')
    .optional(),
  snapshotStagingPath: z
    .string({
      invalid_type_error: 'Snapshot staging path must be a string',
    })
    .max(255, 'Snapshot staging path must be less than 256 characters')
    .optional(),
  cdcStagingPath: z
    .string({
      invalid_type_error: 'CDC staging path must be a string',
    })
    .max(255, 'CDC staging path must be less than 256 characters')
    .optional(),
  softDelete: z.boolean().optional(),
});

export const qrepSchema = z.object({
  sourceName: z.string({ required_error: 'Source peer is required' }).min(1),
  destinationName: z
    .string({ required_error: 'Destination peer is required' })
    .min(1),
  initialCopyOnly: z.boolean().optional(),
  setupWatermarkTableOnDestination: z.boolean().optional(),
  destinationTableIdentifier: z
    .string({
      invalid_type_error: 'Destination table name must be a string',
      required_error: 'Destination table name is required',
    })
    .min(1, 'Destination table name must be non-empty')
    .max(255, 'Destination table name must be less than 256 characters'),
  watermarkTable: z
    .string({
      invalid_type_error: 'Watermark table must be a string',
      required_error: 'Watermark table is required',
    })
    .min(1, 'Watermark table must be non-empty')
    .max(255, 'Watermark table must be less than 256 characters'),
  watermarkColumn: z
    .string({
      invalid_type_error: 'Watermark column must be a string',
      required_error: 'Watermark column is required',
    })
    .min(1, 'Watermark column must be non-empty')
    .max(255, 'Watermark column must be less than 256 characters'),
  numRowsPerPartition: z
    .number({
      invalid_type_error: 'Rows per partition must be a number',
      required_error: 'Rows per partition is required',
    })
    .int()
    .min(1, 'Rows per partition must be a positive integer'),
  maxParallelWorkers: z
    .number({
      invalid_type_error: 'max workers must be a number',
    })
    .int()
    .min(1, 'max workers must be a positive integer')
    .optional(),
  stagingPath: z
    .string({
      invalid_type_error: 'Staging path must be a string',
    })
    .max(255, 'Staging path must be less than 256 characters')
    .optional(),
  writeMode: z.object(
    {
      writeType: z
        .number({ required_error: 'Write type is required' })
        .int()
        .min(0)
        .max(2),
      upsert_key_columns: z.array(z.string()).optional(),
    },
    { required_error: 'Write mode is required' }
  ),
  waitBetweenBatchesSeconds: z
    .number({
      invalid_type_error: 'Batch wait must be a number',
    })
    .int()
    .min(1, 'Batch wait must be a non-negative integer')
    .optional(),
});
