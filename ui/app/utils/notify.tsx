import Link from 'next/link';
import { toast } from 'react-toastify';

const orderingKeyDoc = 'https://docs.peerdb.io/mirror/ordering-key-different';
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

// TODO: add a link to the document when ready
const SortingKeyToast = () => {
  return (
    <div>
      <p>
        Using ordering keys in ClickHouse that differ from the primary key in
        Postgres has some caveats. Please read{' '}
        <Link style={{ color: 'teal' }} href={orderingKeyDoc} target='_blank'>
          this doc
        </Link>{' '}
        carefully.
      </p>
    </div>
  );
};

export const notifySortingKey = () => {
  toast.warn(SortingKeyToast, {
    position: 'bottom-center',
    autoClose: false,
    closeOnClick: false,
    closeButton: true,
    toastId: 'sorting_key_warning',
  });
};
