import {
  PeerConfig,
  UCreatePeerResponse,
  UValidatePeerResponse,
} from '@/app/dto/PeersDTO';
import { S3Config } from '@/grpc_generated/peers';
import { Dispatch, SetStateAction } from 'react';
import {
  bqSchema,
  peerNameSchema,
  pgSchema,
  s3Schema,
  sfSchema,
} from './schema';

const validateFields = (
  type: string,
  config: PeerConfig,
  setMessage: Dispatch<SetStateAction<{ ok: boolean; msg: string }>>,
  name?: string
): boolean => {
  const peerNameValid = peerNameSchema.safeParse(name);
  if (!peerNameValid.success) {
    const peerNameErr = peerNameValid.error.issues[0].message;
    setMessage({ ok: false, msg: peerNameErr });
    return false;
  }

  if (type === 'S3') {
    const s3Valid = S3Validation(config as S3Config);
    if (s3Valid.length > 0) {
      setMessage({ ok: false, msg: s3Valid });
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
    case 'S3':
      const s3Config = s3Schema.safeParse(config);
      if (!s3Config.success) validationErr = s3Config.error.issues[0].message;
      break;
    default:
      validationErr = 'Unsupported peer type ' + type;
  }
  if (validationErr) {
    setMessage({ ok: false, msg: validationErr });
    return false;
  } else setMessage({ ok: true, msg: '' });
  return true;
};

// API call to validate peer
export const handleValidate = async (
  type: string,
  config: PeerConfig,
  setMessage: Dispatch<SetStateAction<{ ok: boolean; msg: string }>>,
  setLoading: Dispatch<SetStateAction<boolean>>,
  name?: string
) => {
  const isValid = validateFields(type, config, setMessage, name);
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
  }).then((res) => res.json());
  if (!valid.valid) {
    setMessage({ ok: false, msg: valid.message });
    setLoading(false);
    return;
  }
  setMessage({ ok: true, msg: 'Peer is valid' });
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
  setMessage: Dispatch<SetStateAction<{ ok: boolean; msg: string }>>,
  setLoading: Dispatch<SetStateAction<boolean>>,
  route: RouteCallback,
  name?: string
) => {
  let isValid = validateFields(type, config, setMessage, name);
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
  }).then((res) => res.json());
  if (!createdPeer.created) {
    setMessage({ ok: false, msg: createdPeer.message });
    setLoading(false);
    return;
  }
  setMessage({ ok: true, msg: 'Peer created successfully' });
  route();
  setLoading(false);
};
