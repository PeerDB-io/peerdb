import { useSyncExternalStore } from 'react';

const subscribe = () => () => {};
const getClientSnapshot = () => true;
const getServerSnapshot = () => false;

export default function useHydrated() {
  return useSyncExternalStore(
    subscribe,
    getClientSnapshot,
    getServerSnapshot
  );
}
