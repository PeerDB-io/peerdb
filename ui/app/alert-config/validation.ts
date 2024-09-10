import z from 'zod';

const baseServiceConfigSchema = z.object({
  slot_lag_mb_alert_threshold: z
    .number({
      invalid_type_error: 'Slot threshold must be a number',
    })
    .min(0, 'Slot threshold must be non-negative'),
  open_connections_alert_threshold: z
    .number({
      invalid_type_error: 'Threshold must be a number',
    })
    .int({ message: 'Connections threshold must be a valid integer' })
    .min(0, 'Connections threshold must be non-negative'),
});

export const slackServiceConfigSchema = z.intersection(
  baseServiceConfigSchema,
  z.object({
    auth_token: z
      .string({ required_error: 'Auth Token is needed.' })
      .min(1, { message: 'Auth Token cannot be empty' })
      .max(256, { message: 'Auth Token is too long' }),
    channel_ids: z
      .array(
        z.string().trim().min(1, { message: 'Channel IDs cannot be empty' }),
        {
          required_error: 'We need a channel ID',
        }
      )
      .min(1, { message: 'Atleast one channel ID is needed' }),
    members: z.array(z.string().trim()).optional(),
  })
);

export const emailServiceConfigSchema = z.intersection(
  baseServiceConfigSchema,
  z.object({
    email_addresses: z
      .array(
        z
          .string()
          .trim()
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
  id: z.optional(z.number({ invalid_type_error: 'ID must be a valid number' })),
  serviceType: z.enum(['slack', 'email'], {
    errorMap: (issue, ctx) => ({ message: 'Invalid service type' }),
  }),
  serviceConfig: serviceConfigSchema,
  alertForMirrors: z.array(z.string().trim()).optional(),
});

export type baseServiceConfigType = z.infer<typeof baseServiceConfigSchema>;

export type slackConfigType = z.infer<typeof slackServiceConfigSchema>;
export type emailConfigType = z.infer<typeof emailServiceConfigSchema>;

export type serviceConfigType = z.infer<typeof serviceConfigSchema>;

export type alertConfigType = z.infer<typeof alertConfigReqSchema>;

export const serviceTypeSchemaMap = {
  slack: slackServiceConfigSchema,
  email: emailServiceConfigSchema,
};
