import { DBType } from '@/grpc_generated/peers';

export const typeMap = (type: string) => {
  switch (type) {
    case 'POSTGRES':
      return DBType.POSTGRES;
    case 'SNOWFLAKE':
      return DBType.SNOWFLAKE;
    default:
      return DBType.UNRECOGNIZED;
  }
};
