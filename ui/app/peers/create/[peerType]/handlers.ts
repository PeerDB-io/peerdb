import {
  PeerConfig,
  UCreatePeerResponse,
  UValidatePeerResponse,
} from '@/app/dto/PeersDTO';
import { S3Config } from '@/grpc_generated/peers';
import { Dispatch, SetStateAction } from 'react';
import {
  bqSchema,
  chSchema,
  peerNameSchema,
  pgSchema,
  s3Schema,
  sfSchema,
} from './schema';

const validateFields = (
  type: string,
  config: PeerConfig,
  notify: (msg: string) => void,
  name?: string
): boolean => {
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
      const chConfig = chSchema.safeParse(config);
      if (!chConfig.success) validationErr = chConfig.error.issues[0].message;
      break;
    case 'S3':
      const s3Config = s3Schema.safeParse(config);
      if (!s3Config.success) validationErr = s3Config.error.issues[0].message;
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
  const isValid = validateFields(type, config, notify, name);
  if (!isValid) return;
  setLoading(true);
  const valid: UValidatePeerResponse = await fetch('/api/peers/', {
    method: 'POST',
    body: JSON.stringify({
      name,
      type,
      config,
      mode: 'validate',
    }),
    cache: 'no-store',
  }).then((res) => res.json());
  if (!valid.valid) {
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
export const handleCreate = async (
  type: string,
  config: PeerConfig,
  notify: (msg: string) => void,
  setLoading: Dispatch<SetStateAction<boolean>>,
  route: RouteCallback,
  name?: string
) => {
  let isValid = validateFields(type, config, notify, name);
  if (!isValid) return;
  setLoading(true);
  const createdPeer: UCreatePeerResponse = await fetch('/api/peers/', {
    method: 'POST',
    body: JSON.stringify({
      name,
      type,
      config,
      mode: 'create',
    }),
    cache: 'no-store',
  }).then((res) => res.json());
  if (!createdPeer.created) {
    notify(createdPeer.message);
    setLoading(false);
    return;
  }

  route();
  setLoading(false);
};
