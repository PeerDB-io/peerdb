import { ScriptsType } from '../dto/ScriptsDTO';
import { notifyErr } from '../utils/notify';

export const AddScript = async (script: ScriptsType) => {
  try {
    const addScriptRes = await fetch('/api/scripts', {
      method: 'POST',
      body: JSON.stringify(script),
    });
    const addScriptStatus = await addScriptRes.text();
    if (addScriptStatus === 'error') {
      notifyErr(
        'Something went wrong when adding the script. Please try again'
      );
      return false;
    }
  } catch (err) {
    notifyErr('Something went wrong when adding the script. Please try again');
    return false;
  }

  notifyErr('Successfully added script', true);
  return true;
};

export const HandleEditScript = async (script: ScriptsType) => {
  try {
    const editScriptRes = await fetch('/api/scripts', {
      method: 'PUT',
      body: JSON.stringify(script),
    });
    const addScriptStatus = await editScriptRes.text();
    if (addScriptStatus === 'error') {
      notifyErr(
        'Something went wrong when editing the script. Please try again'
      );
      return false;
    }
  } catch (err) {
    notifyErr('Something went wrong when editing the script. Please try again');
    return false;
  }
  notifyErr('Successfully edited script', true);
  return true;
};

export const DeleteScript = async (scriptId: number) => {
  try {
    const deleteScriptRes = await fetch('/api/scripts', {
      method: 'DELETE',
      body: JSON.stringify(scriptId),
    });
    const addScriptStatus = await deleteScriptRes.text();
    if (addScriptStatus === 'error') {
      notifyErr(
        'Something went wrong when deleting the script. Please try again'
      );
      return false;
    }
  } catch (err) {
    notifyErr(
      'Something went wrong when deleting the script. Please try again'
    );
    return false;
  }
  notifyErr('Successfully deleted script', true);
  return true;
};
