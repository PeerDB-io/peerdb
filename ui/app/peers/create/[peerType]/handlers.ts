import { PeerConfig } from '@/app/dto/PeersDTO';
import {
  BigqueryConfig,
  ClickhouseConfig,
  DBType,
  ElasticsearchConfig,
  EventHubGroupConfig,
  KafkaConfig,
  Peer,
  PostgresConfig,
  PubSubConfig,
  S3Config,
  SnowflakeConfig,
} from '@/grpc_generated/peers';
import {
  CreatePeerRequest,
  CreatePeerResponse,
  CreatePeerStatus,
  createPeerStatusFromJSON,
  ValidatePeerRequest,
  ValidatePeerResponse,
  ValidatePeerStatus,
  validatePeerStatusFromJSON,
} from '@/grpc_generated/route';
import { Dispatch, SetStateAction } from 'react';

import {
  bqSchema,
  chSchema,
  ehGroupSchema,
  esSchema,
  kaSchema,
  peerNameSchema,
  pgSchema,
  psSchema,
  s3Schema,
  sfSchema,
} from './schema';

function constructPeer(
  name: string,
  type: string,
  config: PeerConfig
): Peer | undefined {
  switch (type) {
    case 'POSTGRES':
      return {
        name,
        type: DBType.POSTGRES,
        postgresConfig: config as PostgresConfig,
      };
    case 'SNOWFLAKE':
      return {
        name,
        type: DBType.SNOWFLAKE,
        snowflakeConfig: config as SnowflakeConfig,
      };
    case 'BIGQUERY':
      return {
        name,
        type: DBType.BIGQUERY,
        bigqueryConfig: config as BigqueryConfig,
      };
    case 'CLICKHOUSE':
      return {
        name,
        type: DBType.CLICKHOUSE,
        clickhouseConfig: config as ClickhouseConfig,
      };
    case 'S3':
      return {
        name,
        type: DBType.S3,
        s3Config: config as S3Config,
      };
    case 'KAFKA':
      return {
        name,
        type: DBType.KAFKA,
        kafkaConfig: config as KafkaConfig,
      };
    case 'PUBSUB':
      return {
        name,
        type: DBType.PUBSUB,
        pubsubConfig: config as PubSubConfig,
      };
    case 'EVENTHUBS':
      return {
        name,
        type: DBType.EVENTHUBS,
        eventhubGroupConfig: config as EventHubGroupConfig,
      };
    case 'ELASTICSEARCH':
      return {
        name,
        type: DBType.ELASTICSEARCH,
        elasticsearchConfig: config as ElasticsearchConfig,
      };
    default:
      return;
  }
}

const GetClickHouseAllowedDomains = async (): Promise<string[]> => {
  const response = await fetch('/api/peer-types/validation/clickhouse', {
    method: 'GET',
    cache: 'force-cache',
  });
  const hostDomains: string[] = await response.json();
  return hostDomains;
};

const validateFields = async (
  type: string,
  config: PeerConfig,
  notify: (msg: string) => void,
  name?: string
): Promise<boolean> => {
  const peerNameValid = peerNameSchema.safeParse(name);
  if (!peerNameValid.success) {
    const peerNameErr = peerNameValid.error.issues[0].message;
    notify(peerNameErr);
    return false;
  }

  if (type === 'S3') {
    const s3Valid = S3Validation(config as S3Config);
    if (s3Valid.length > 0) {
      notify(s3Valid);
      return false;
    }
  }

  let validationErr: string | undefined;
  switch (type) {
    case 'POSTGRES':
      const pgConfig = pgSchema.safeParse(config);
      if (!pgConfig.success) validationErr = pgConfig.error.issues[0].message;
      break;
    case 'SNOWFLAKE':
      const sfConfig = sfSchema.safeParse(config);
      if (!sfConfig.success) validationErr = sfConfig.error.issues[0].message;
      break;
    case 'BIGQUERY':
      const bqConfig = bqSchema.safeParse(config);
      if (!bqConfig.success) validationErr = bqConfig.error.issues[0].message;
      break;
    case 'CLICKHOUSE':
      const chAllowedDomains = await GetClickHouseAllowedDomains();
      const chConfig = chSchema(chAllowedDomains).safeParse(config);
      if (!chConfig.success) validationErr = chConfig.error.issues[0].message;
      break;
    case 'S3':
      const s3Config = s3Schema.safeParse(config);
      if (!s3Config.success) validationErr = s3Config.error.issues[0].message;
      break;
    case 'KAFKA':
      const kaConfig = kaSchema.safeParse(config);
      if (!kaConfig.success) validationErr = kaConfig.error.issues[0].message;
      break;
    case 'PUBSUB':
      const psConfig = psSchema.safeParse(config);
      if (!psConfig.success) validationErr = psConfig.error.issues[0].message;
      break;
    case 'EVENTHUBS':
      const ehGroupConfig = ehGroupSchema.safeParse(config);
      if (!ehGroupConfig.success)
        validationErr = ehGroupConfig.error.issues[0].message;
      break;
    case 'ELASTICSEARCH':
      const esConfig = esSchema.safeParse(config);
      if (!esConfig.success) validationErr = esConfig.error.issues[0].message;
      break;
    default:
      validationErr = 'Unsupported peer type ' + type;
  }
  if (validationErr) {
    notify(validationErr);
    return false;
  }
  return true;
};

// API call to validate peer
export const handleValidate = async (
  type: string,
  config: PeerConfig,
  notify: (msg: string, success?: boolean) => void,
  setLoading: Dispatch<SetStateAction<boolean>>,
  name?: string
) => {
  const isValid = await validateFields(type, config, notify, name);
  if (!isValid) return;
  setLoading(true);

  const validateReq: ValidatePeerRequest = {
    peer: constructPeer(name!, type, config),
  };
  const valid: ValidatePeerResponse = await fetch('/api/v1/peers/validate', {
    method: 'POST',
    body: JSON.stringify(validateReq),
    cache: 'no-store',
  }).then((res) => res.json());
  if (validatePeerStatusFromJSON(valid.status) !== ValidatePeerStatus.VALID) {
    notify(valid.message);
    setLoading(false);
    return;
  }
  notify('Peer is valid', true);
  setLoading(false);
};

const S3Validation = (config: S3Config): string => {
  if (!config.secretAccessKey && !config.accessKeyId && !config.roleArn) {
    return 'Either both access key and secret or role ARN is required';
  }
  return '';
};

// API call to create peer
export async function handleCreate(
  type: string,
  config: PeerConfig,
  notify: (msg: string) => void,
  setLoading: Dispatch<SetStateAction<boolean>>,
  route: RouteCallback,
  name?: string
) {
  let isValid = await validateFields(type, config, notify, name);
  if (!isValid) return;
  setLoading(true);
  const req: CreatePeerRequest = {
    peer: constructPeer(name!, type, config),
    allowUpdate: true,
  };
  const createdPeer: CreatePeerResponse = await fetch('/api/v1/peers/create', {
    method: 'POST',
    body: JSON.stringify(req),
    cache: 'no-store',
  }).then((res) => res.json());
  if (
    createPeerStatusFromJSON(createdPeer.status) !== CreatePeerStatus.CREATED
  ) {
    notify(createdPeer.message);
    setLoading(false);
    return;
  }

  route();
  setLoading(false);
}
