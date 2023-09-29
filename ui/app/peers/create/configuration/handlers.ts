import { Dispatch, SetStateAction } from 'react';
import { checkFormFields } from './schema';
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

  const validity = checkFormFields(type, config);
  if (validity.error) {
    setMessage({ ok: false, msg: validity.error.message });
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
  const statusMessage = await fetch('/api/peers/validate', {
    method: 'POST',
    body: JSON.stringify({
      name,
      type,
      config,
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
  name?: string
) => {
  let isValid = validateFields(type, config, setMessage, name);
  if (!isValid) return;
  setLoading(true);
  const statusMessage = await fetch('/api/peers/create', {
    method: 'POST',
    body: JSON.stringify({
      name,
      type,
      config,
    }),
  }).then((res) => res.text());

  if (statusMessage !== 'created') {
    setMessage({ ok: false, msg: statusMessage });
    setLoading(false);
    return;
  } else {
    setMessage({ ok: true, msg: 'Peer created successfully' });
  }
  setLoading(false);
};
