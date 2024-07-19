import {
  GetScriptsResponse,
  PostScriptRequest,
  Script,
} from '@/grpc_generated/route';
import { notifyErr } from '../utils/notify';

export const HandleAddScript = async (script: Script) => {
  const addScriptRes = await fetch('/api/v1/scripts', {
    method: 'POST',
    body: JSON.stringify({
      script: {
        ...script,
        id: -1,
      },
    } as PostScriptRequest),
  });

  if (!addScriptRes.ok) {
    notifyErr('Something went wrong when adding the script. Please try again');
    return false;
  }
  notifyErr('Successfully added script', true);
  return true;
};

export const GetScriptById = async (scriptId: string) => {
  try {
    const scriptByIdRes = await fetch(`/api/v1/scripts/${scriptId}`);
    const scriptRes: GetScriptsResponse = await scriptByIdRes.json();
    if (!scriptRes) {
      notifyErr('Script not found');
      return;
    }
    return scriptRes.scripts.at(0);
  } catch (err) {
    notifyErr(
      'Something went wrong when obtaining the existing script. Please try again'
    );
    return;
  }
};

export const HandleEditScript = async (script: Script) => {
  const editScriptRes = await fetch('/api/v1/scripts', {
    method: 'POST',
    body: JSON.stringify({
      script,
    } as PostScriptRequest),
  });

  if (!editScriptRes.ok) {
    notifyErr('Something went wrong when editing the script. Please try again');
    return false;
  }
  notifyErr('Successfully edited script', true);
  return true;
};

export const DeleteScript = async (scriptId: number) => {
  const deleteScriptRes = await fetch(`/api/v1/scripts/${scriptId}`, {
    method: 'DELETE',
  });
  if (!deleteScriptRes.ok) {
    notifyErr(
      'Something went wrong when deleting the script. Please try again'
    );
    return false;
  }

  notifyErr('Successfully deleted script', true);
  return true;
};
