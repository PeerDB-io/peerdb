import * as z from 'zod';

export const ehSchema = z.object({
  namespace: z
    .string({
      required_error: 'Namespace is required',
      invalid_type_error: 'Namespace must be a string',
    })
    .min(1, { message: 'Namespace cannot be empty' })
    .max(255, { message: 'Namespace cannot be longer than 255 characters' }),
  subscriptionId: z
    .string({
      required_error: 'Subscription ID is required',
      invalid_type_error: 'Subscription ID must be a string',
    })
    .min(1, { message: 'Subscription ID cannot be empty' })
    .max(255, {
      message: 'Subscription ID cannot be longer than 255 characters',
    }),
  resourceGroup: z
    .string({
      required_error: 'Resource Group is required',
      invalid_type_error: 'Resource Group must be a string',
    })
    .min(1, { message: 'Resource Group cannot be empty' })
    .max(255, {
      message: 'Resource Group cannot be longer than 255 characters',
    }),
  location: z
    .string({
      required_error: 'Location is required',
      invalid_type_error: 'Location must be a string',
    })
    .min(1, { message: 'Location cannot be empty' })
    .max(255, { message: 'Location cannot be longer than 255 characters' }),
  partitionCount: z
    .number({
      required_error: 'Partition Count is required',
      invalid_type_error: 'Partition Count must be a number',
    })
    .int({ message: 'Partition Count must be an integer' })
    .min(1, { message: 'Partition count must be at least 1' }),
  messageRetentionInDays: z
    .number({
      required_error: 'Message Retention (Days) is required',
      invalid_type_error: 'Message Retention (Days) must be a number',
    })
    .int({ message: 'Message Retention (Days) must be an integer' })
    .min(1, { message: 'Message retention (days) must be at least 1' }),
});
