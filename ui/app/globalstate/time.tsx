import { create } from 'zustand';

interface TZState {
  timezone: string;
  setZone: (tz: string) => void;
}

const useTZStore = create<TZState>()((set) => ({
  timezone: 'UTC',
  setZone: (tz) => set(() => ({ timezone: tz })),
}));

export default useTZStore;
