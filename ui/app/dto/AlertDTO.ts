export enum LogType {
  ERROR = 'ERROR',
  WARNING = 'WARNING',
  INFO = 'INFO',
  ALL = 'ALL',
}

export type MirrorLogsRequest = {
  flowJobName: string;
  natureOfLog?: LogType;
  page: number;
  numPerPage: number;
};

export type MirrorLog = {
  flow_name: string;
  error_message: string;
  error_type: string;
  error_timestamp: Date;
};

export type MirrorLogsResponse = {
  errors: MirrorLog[];
  total: number;
};
