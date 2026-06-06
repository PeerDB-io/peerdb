import * as z from 'zod/v4';

// Tags fields the backend redacts; the edit schema and display masking derive
// from it. Backend mirror: RedactSecrets/MergeSecrets in flow/alerting/secrets.go.
const secretRegistry = z.registry<{ secret: boolean }>();

const baseServiceConfigSchema = z.object({
  slot_lag_mb_alert_threshold: z
    .number({
      error: () => 'Slot threshold must be a number',
    })
    .min(0, 'Slot threshold must be non-negative'),
  open_connections_alert_threshold: z
    .int({ error: () => 'Threshold must be an integer' })
    .min(0, 'Connections threshold must be non-negative'),
});

export const slackServiceConfigSchema = baseServiceConfigSchema.extend({
  auth_token: z
    .string({ error: () => 'Auth Token is needed.' })
    .min(1, { message: 'Auth Token cannot be empty' })
    .max(256, { message: 'Auth Token is too long' })
    .register(secretRegistry, { secret: true }),
  channel_ids: z
    .array(z.string().trim().min(1, { message: 'Channel IDs cannot be empty' }))
    .min(1, { message: 'At least one channel ID is needed' }),
  members: z.array(z.string().trim()).optional(),
});

export const emailServiceConfigSchema = baseServiceConfigSchema.extend({
  email_addresses: z
    .array(
      z
        .string()
        .trim()
        .min(1, { message: 'Email Addresses cannot be empty' })
        .includes('@')
    )
    .min(1, { message: 'At least one email address is needed' }),
});

// getSecretFields returns the JSON keys of fields tagged secret in the schema.
function getSecretFields(schema: z.ZodObject): string[] {
  return Object.entries(schema.shape)
    .filter(([, field]) => secretRegistry.get(field as z.ZodType)?.secret)
    .map(([name]) => name);
}

// makeEditSchema relaxes every secret field to also accept an empty string,
// which the backend reads as "keep the stored value". The API never returns
// the current secret to the UI, so on edit it starts blank.
function makeEditSchema<T extends z.ZodObject>(schema: T): T {
  const relaxed: Record<string, z.ZodType> = {};
  for (const name of getSecretFields(schema)) {
    relaxed[name] = z.union([schema.shape[name] as z.ZodType, z.literal('')]);
  }
  return schema.extend(relaxed) as unknown as T;
}

export const serviceConfigSchema = z.union([
  slackServiceConfigSchema,
  emailServiceConfigSchema,
]);

const serviceConfigReqSchema = z.union([
  makeEditSchema(slackServiceConfigSchema),
  makeEditSchema(emailServiceConfigSchema),
]);
export const alertConfigReqSchema = z.object({
  id: z.optional(z.number({ error: () => 'ID must be a valid number' })),
  serviceType: z.enum(['slack', 'email'], {
    error: () => ({ message: 'Invalid service type' }),
  }),
  serviceConfig: serviceConfigReqSchema,
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

export const serviceTypeEditSchemaMap = {
  slack: makeEditSchema(slackServiceConfigSchema),
  email: makeEditSchema(emailServiceConfigSchema),
};

// Secret JSON keys per service type, derived from the schema annotations
export const secretFieldsByServiceType: Record<string, string[]> = {
  slack: getSecretFields(slackServiceConfigSchema),
  email: getSecretFields(emailServiceConfigSchema),
};
