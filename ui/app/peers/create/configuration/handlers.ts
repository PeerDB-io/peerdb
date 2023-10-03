import { AppRouterInstance } from 'next/dist/shared/lib/app-router-context';
import { Dispatch, SetStateAction } from 'react';
import { pgSchema } from './schema';
import { PeerConfig } from './types';

// Frontend form validation
const validateFields = (
  type: string,
  config: PeerConfig,
  setMessage: Dispatch<SetStateAction<{ ok: boolean; msg: string }>>,
  name?: string
): boolean => {
  if (!name) {
    setMessage({ ok: false, msg: 'Peer name is required' });
    return false;
  }
  let validationErr: string | undefined;
  switch (type) {
    case 'POSTGRES':
      const pgConfig = pgSchema.safeParse(config);
      if (!pgConfig.success) validationErr = pgConfig.error.issues[0].message;
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
  const statusMessage = await fetch('/api/peers/', {
    method: 'POST',
    body: JSON.stringify({
      name,
      type,
      config,
      mode: 'validate',
    }),
  }).then((res) => res.text());
  if (statusMessage !== 'valid') {
    setMessage({ ok: false, msg: statusMessage });
    setLoading(false);
    return;
  } else {
    setMessage({ ok: true, msg: 'Peer is valid' });
  }
  setLoading(false);
};

// API call to create peer
export const handleCreate = async (
  type: string,
  config: PeerConfig,
  setMessage: Dispatch<SetStateAction<{ ok: boolean; msg: string }>>,
  setLoading: Dispatch<SetStateAction<boolean>>,
  router: AppRouterInstance,
  name?: string
) => {
  let isValid = validateFields(type, config, setMessage, name);
  if (!isValid) return;
  setLoading(true);
  const statusMessage = await fetch('/api/peers/', {
    method: 'POST',
    body: JSON.stringify({
      name,
      type,
      config,
      mode: 'create',
    }),
  }).then((res) => res.text());
  if (statusMessage !== 'created') {
    setMessage({ ok: false, msg: statusMessage });
    setLoading(false);
    return;
  } else {
    setMessage({ ok: true, msg: 'Peer created successfully' });
    router.push('/peers');
  }
  setLoading(false);
};
