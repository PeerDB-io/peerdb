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

const SortingKeyToast = () => (
  <div>
    
  </div>
);

export const notifySortingKey = (msg: string) => {
  toast.warn(msg, {
    position: 'bottom-center',
    autoClose: false,
    closeOnClick: false,
    closeButton: true
  });
}