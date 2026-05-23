import {
  Dispatch,
  SetStateAction,
  useCallback,
  useMemo,
  useSyncExternalStore,
} from 'react';

const localStorageChangeEvent = 'peerdb-local-storage';
const snapshotCache = new Map<string, { raw: string; value: unknown }>();

function readLocalStorageValue<T>(key: string, defaultValue: T): T {
  if (typeof window === 'undefined') return defaultValue;

  const raw = window.localStorage.getItem(key);
  if (raw === null) return defaultValue;

  const cached = snapshotCache.get(key);
  if (cached?.raw === raw) return cached.value as T;

  try {
    const value = JSON.parse(raw) as T;
    snapshotCache.set(key, { raw, value });
    return value;
  } catch {
    return defaultValue;
  }
}

export default function useLocalStorage<T>(
  key: string,
  defaultValue: T
): [T, Dispatch<SetStateAction<T>>] {
  // Treat defaultValue like a useState initializer: stable for this key.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  const stableDefaultValue = useMemo(() => defaultValue, [key]);

  const subscribe = useCallback(
    (onStoreChange: () => void) => {
      if (typeof window === 'undefined') return () => {};

      const handleStorageChange = (event: StorageEvent) => {
        if (event.key === key || event.key === null) onStoreChange();
      };
      const handleLocalStorageChange = (event: Event) => {
        if (
          event instanceof CustomEvent &&
          (event.detail?.key === key || event.detail?.key === null)
        ) {
          onStoreChange();
        }
      };

      window.addEventListener('storage', handleStorageChange);
      window.addEventListener(
        localStorageChangeEvent,
        handleLocalStorageChange
      );

      return () => {
        window.removeEventListener('storage', handleStorageChange);
        window.removeEventListener(
          localStorageChangeEvent,
          handleLocalStorageChange
        );
      };
    },
    [key]
  );

  const getSnapshot = useCallback(
    () => readLocalStorageValue(key, stableDefaultValue),
    [key, stableDefaultValue]
  );
  const getServerSnapshot = useCallback(
    () => stableDefaultValue,
    [stableDefaultValue]
  );

  const value = useSyncExternalStore(subscribe, getSnapshot, getServerSnapshot);

  const setValue = useCallback<Dispatch<SetStateAction<T>>>(
    (nextValue) => {
      if (typeof window === 'undefined') return;

      const currentValue = readLocalStorageValue(key, stableDefaultValue);
      const value =
        typeof nextValue === 'function'
          ? (nextValue as (prev: T) => T)(currentValue)
          : nextValue;
      const raw = JSON.stringify(value);

      window.localStorage.setItem(key, raw);
      snapshotCache.set(key, { raw, value });
      window.dispatchEvent(
        new CustomEvent(localStorageChangeEvent, { detail: { key } })
      );
    },
    [key, stableDefaultValue]
  );

  return [value, setValue];
}
