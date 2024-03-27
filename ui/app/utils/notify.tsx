import { toast } from 'react-toastify';

export const notifyErr = (msg: string, ok?: boolean) => {
  if (ok) {
    toast.success(msg, {
      position: 'bottom-center',
    });
  } else {
    toast.error(msg, {
      position: 'bottom-center',
    });
  }
};
