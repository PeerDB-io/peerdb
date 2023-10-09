import * as z from 'zod';

export const tableMappingSchema = z
  .array(
    z.object({
      source: z
        .string()
        .min(1, 'source table names, if added, must be non-empty'),
      destination: z
        .string()
        .min(1, 'destination table names, if added, must be non-empty'),
    })
  )
  .nonempty('At least one table mapping is required');

export const cdcSchema = z.object({
  source: z.object({
    name: z.string().nonempty(),
    type: z.any(),
    config: z.any(),
  }),
  destination: z.object({
    name: z.string().nonempty(),
    type: z.any(),
    config: z.any(),
  }),
  doInitialCopy: z.boolean().optional(),
  publicationName: z
    .string({
      invalid_type_error: 'Publication name must be a string',
    })
    .max(255, 'Publication name must be less than 255 characters')
    .optional(),
  replicationSlotName: z
    .string({
      invalid_type_error: 'Publication name must be a string',
    })
    .max(255, 'Publication name must be less than 255 characters')
    .optional(),
  snapshotNumRowsPerPartition: z
    .number({
      invalid_type_error: 'Snapshow rows per partition must be a number',
    })
    .int()
    .min(1, 'Snapshow rows per partition must be a positive integer')
    .optional(),
  snapshotMaxParallelWorkers: z
    .number({
      invalid_type_error: 'Snapshow max workers must be a number',
    })
    .int()
    .min(1, 'Snapshow max workers must be a positive integer')
    .optional(),
  snapshotNumTablesInParallel: z
    .number({
      invalid_type_error: 'Snapshow parallel tables must be a number',
    })
    .int()
    .min(1, 'Snapshow parallel tables must be a positive integer')
    .optional(),
  snapshotStagingPath: z
    .string({
      invalid_type_error: 'Snapshot staging path must be a string',
    })
    .max(255, 'Snapshot staging path must be less than 255 characters')
    .optional(),
  cdcStagingPath: z
    .string({
      invalid_type_error: 'CDC staging path must be a string',
    })
    .max(255, 'CDC staging path must be less than 255 characters')
    .optional(),
  softDelete: z.boolean().optional(),
});
