import { Dispatch, SetStateAction, useEffect, useState } from 'react';

export default function useLocalStorage<T>(
  key: string,
  defaultValue: T
): [T, Dispatch<SetStateAction<T>>] {
  const [value, setValue] = useState(() => {
    if (typeof localStorage === 'undefined') return defaultValue;
    const json = localStorage.getItem(key);
    if (!json) return defaultValue;
    return JSON.parse(json);
  });

  useEffect(() => {
    if (typeof localStorage !== 'undefined')
      localStorage.setItem(key, JSON.stringify(value));
  }, [key, value]);

  return [value, setValue];
}
