import z from 'zod';

const baseServiceConfigSchema = z.object({
  slot_lag_mb_alert_threshold: z
    .number({
      invalid_type_error: 'Threshold must be a number',
    })
    .int()
    .min(0, 'Threshold must be non-negative'),
  open_connections_alert_threshold: z
    .number({
      invalid_type_error: 'Threshold must be a number',
    })
    .int()
    .min(0, 'Threshold must be non-negative'),
});

const slackServiceConfigSchema = z.intersection(
  baseServiceConfigSchema,
  z.object({
    auth_token: z
      .string({ required_error: 'Auth Token is needed.' })
      .min(1, { message: 'Auth Token cannot be empty' })
      .max(256, { message: 'Auth Token is too long' }),
    channel_ids: z
      .array(z.string().min(1, { message: 'Channel IDs cannot be empty' }), {
        required_error: 'We need a channel ID',
      })
      .min(1, { message: 'Atleast one channel ID is needed' }),
  })
);

const emailServiceConfigSchema = z.intersection(
  baseServiceConfigSchema,
  z.object({
    email_addresses: z
      .array(
        z
          .string()
          .min(1, { message: 'Email Addresses cannot be empty' })
          .includes('@'),
        {
          required_error: 'We need an Email Address',
        }
      )
      .min(1, { message: 'Atleast one email address is needed' }),
  })
);

export const serviceConfigSchema = z.union([
  slackServiceConfigSchema,
  emailServiceConfigSchema,
]);
export const alertConfigReqSchema = z.object({
  id: z.optional(z.number()),
  serviceType: z.enum(['slack', 'email'], {
    errorMap: (issue, ctx) => ({ message: 'Invalid service type' }),
  }),
  serviceConfig: serviceConfigSchema,
});

export type baseServiceConfigType = z.infer<typeof baseServiceConfigSchema>;

export type slackConfigType = z.infer<typeof slackServiceConfigSchema>;
export type emailConfigType = z.infer<typeof emailServiceConfigSchema>;

export type serviceConfigType = z.infer<typeof serviceConfigSchema>;

export type alertConfigType = z.infer<typeof alertConfigReqSchema>;
