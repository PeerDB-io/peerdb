import * as z from 'zod/v4';

export const flowNameSchema = z
  .string({
    error: (issue) =>
      issue.input === undefined
        ? 'Mirror name is required.'
        : 'Mirror name is invalid.',
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
        code: 'custom',
        message: `Two source tables have been mapped to the same destination table`,
      });
    }
  });

export const cdcSchema = z.object({
  sourceName: z.string({ error: 'Source peer is required' }).min(1),
  destinationName: z.string({ error: 'Destination peer is required' }).min(1),
  doInitialCopy: z.boolean().optional(),
  publicationName: z
    .string({ error: 'Publication name must be a string' })
    .max(255, 'Publication name must be less than 256 characters')
    .optional(),
  replicationSlotName: z
    .string({
      error: 'Replication slot name must be a string',
    })
    .max(255, 'Replication slot name must be less than 256 characters')
    .optional(),
  snapshotNumRowsPerPartition: z
    .int({
      error: 'Snapshot rows per partition must be a number',
    })
    .min(1, 'Snapshot rows per partition must be a positive integer')
    .optional(),
  snapshotMaxParallelWorkers: z
    .int({
      error: 'Initial load parallelism must be a number',
    })
    .min(1, 'Initial load parallelism must be a positive integer')
    .optional(),
  snapshotNumTablesInParallel: z
    .int({
      error: 'Snapshot parallel tables must be a number',
    })
    .min(1, 'Snapshot parallel tables must be a positive integer')
    .optional(),
  snapshotStagingPath: z
    .string({
      error: 'Snapshot staging path must be a string',
    })
    .max(255, 'Snapshot staging path must be less than 256 characters')
    .optional(),
  cdcStagingPath: z
    .string({
      error: 'CDC staging path must be a string',
    })
    .max(255, 'CDC staging path must be less than 256 characters')
    .optional(),
  softDelete: z.boolean().optional(),
});

export const qrepSchema = z.object({
  sourceName: z.string({ error: 'Source peer is required' }).min(1),
  destinationName: z.string({ error: 'Destination peer is required' }).min(1),
  initialCopyOnly: z.boolean().optional(),
  setupWatermarkTableOnDestination: z.boolean().optional(),
  destinationTableIdentifier: z
    .string({
      error: (issue) =>
        issue.input === undefined
          ? 'Destination table name is required'
          : 'Destination table name must be a string',
    })
    .min(1, 'Destination table name must be non-empty')
    .max(255, 'Destination table name must be less than 256 characters'),
  watermarkTable: z
    .string({
      error: (issue) =>
        issue.input === undefined
          ? 'Watermark table is required'
          : 'Watermark table must be a string',
    })
    .min(1, 'Watermark table must be non-empty')
    .max(255, 'Watermark table must be less than 256 characters'),
  watermarkColumn: z
    .string({
      error: (issue) =>
        issue.input === undefined
          ? 'Watermark column is required'
          : 'Watermark column must be a string',
    })
    .min(1, 'Watermark column must be non-empty')
    .max(255, 'Watermark column must be less than 256 characters'),
  numRowsPerPartition: z
    .int({
      error: (issue) =>
        issue.input === undefined
          ? 'Rows per partition is required'
          : 'Rows per partition must be a number',
    })
    .min(1, 'Rows per partition must be a positive integer'),
  maxParallelWorkers: z
    .int({ error: () => 'max workers must be a number' })
    .min(1, 'max workers must be a positive integer')
    .optional(),
  stagingPath: z
    .string({ error: () => 'Staging path must be a string' })
    .max(255, 'Staging path must be less than 256 characters')
    .optional(),
  writeMode: z.object(
    {
      writeType: z
        .int({ error: () => 'Write type is required' })
        .min(0)
        .max(2),
      upsert_key_columns: z.array(z.string()).optional(),
    },
    { error: 'Write mode is required' }
  ),
  waitBetweenBatchesSeconds: z
    .int({
      error: 'Batch wait must be a number',
    })
    .min(1, 'Batch wait must be a non-negative integer')
    .optional(),
});
