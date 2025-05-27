import * as z from 'zod/v4';

export const ehSchema = z.object({
  namespace: z
    .string({
      error: (issue) =>
        issue.input === undefined
          ? 'Namespace is required'
          : 'Namespace must be a string',
    })
    .min(1, { message: 'Namespace cannot be empty' })
    .max(255, { message: 'Namespace cannot be longer than 255 characters' }),
  subscriptionId: z
    .string({
      error: (issue) =>
        issue.input === undefined
          ? 'Subscription ID is required'
          : 'Subscription ID must be a string',
    })
    .min(1, { message: 'Subscription ID cannot be empty' })
    .max(255, {
      message: 'Subscription ID cannot be longer than 255 characters',
    }),
  resourceGroup: z
    .string({
      error: (issue) =>
        issue.input === undefined
          ? 'Resource Group is required'
          : 'Resource Group must be a string',
    })
    .min(1, { message: 'Resource Group cannot be empty' })
    .max(255, {
      message: 'Resource Group cannot be longer than 255 characters',
    }),
  location: z
    .string({
      error: (issue) =>
        issue.input === undefined
          ? 'Location is required'
          : 'Location must be a string',
    })
    .min(1, { message: 'Location cannot be empty' })
    .max(255, { message: 'Location cannot be longer than 255 characters' }),
  partitionCount: z
    .int({
      error: (issue) =>
        issue.input === undefined
          ? 'Partition Count is required'
          : 'Partition Count must be an integer',
    })
    .min(1, { message: 'Partition count must be at least 1' }),
  messageRetentionInDays: z
    .int({
      error: (issue) =>
        issue.input === undefined
          ? 'Message Retention (Days) is required'
          : 'Message Retention (Days) must be an integer',
    })
    .min(1, { message: 'Message retention (days) must be at least 1' }),
});
