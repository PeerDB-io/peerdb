import { PostScriptRequest, Script } from '@/grpc_generated/route';
import { notifyErr } from '../utils/notify';

export const HandleAddScript = async (script: Script) => {
  const addScriptRes = await fetch('/api/scripts', {
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

export const GetScriptById = async (id: string) => {
  try {
    const scriptByIdRes = await fetch('/api/scripts/' + id);
    const scriptRes: Script = await scriptByIdRes.json();
    if (!scriptRes) {
      notifyErr('Script not found');
      return;
    }
    return scriptRes;
  } catch (err) {
    notifyErr(
      'Something went wrong when obtaining the existing script. Please try again'
    );
    return;
  }
};

export const HandleEditScript = async (script: Script) => {
  const editScriptRes = await fetch('/api/scripts', {
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
  const deleteScriptRes = await fetch(`/api/scripts?id=${scriptId}`, {
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
