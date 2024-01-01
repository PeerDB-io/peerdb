import z from 'zod';

export const alertConfigReqSchema = z.object({
  serviceType: z.enum(['slack'], {
    errorMap: (issue, ctx) => ({ message: 'Invalid service type' }),
  }),
  serviceConfig: z.object({
    authToken: z
      .string({ required_error: 'Auth Token is needed.' })
      .min(1, { message: 'Auth Token cannot be empty' })
      .max(256, { message: 'Auth Token is too long' }),
    channelIds: z
      .array(z.string().min(1, { message: 'Channel IDs cannot be empty' }), {
        required_error: 'We need a channel ID',
      })
      .min(1, { message: 'Atleast one channel ID is needed' }),
  }),
});

export type alertConfigType = z.infer<typeof alertConfigReqSchema>;
