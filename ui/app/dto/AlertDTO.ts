import { Prisma } from '@prisma/client';

export type UAlertConfigResponse = {
  id: bigint;
  service_type: string;
  service_config: Prisma.JsonValue;
};

export type MirrorLogsRequest = {
  flowJobName: string;
  page: number;
  numPerPage: number;
};

export type MirrorLog = {
  flow_name: string;
  error_message: string;
  error_type: string;
  error_timestamp: Date;
  ack: boolean;
};

export type MirrorLogsResponse = {
  errors: MirrorLog[];
  total: number;
};
