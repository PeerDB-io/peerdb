'use client';
import useLocalStorage from '@/app/utils/useLocalStorage';
import { Label } from '@/lib/Label';
import { ProgressCircle } from '@/lib/ProgressCircle';
import moment from 'moment-timezone';
import { useEffect, useState } from 'react';

// Custom hook to handle hydration
function useHydrated() {
  const [hydrated, setHydrated] = useState(false);

  useEffect(() => {
    // Defer the state update to avoid synchronous setState
    const timer = setTimeout(() => setHydrated(true), 0);
    return () => clearTimeout(timer);
  }, []);

  return hydrated;
}

export default function TimeLabel({
  timeVal,
  fontSize,
}: {
  timeVal: Date | string;
  fontSize?: number;
}) {
  const [timezone] = useLocalStorage('timezone-ui', 'UTC');
  const mounted = useHydrated();

  const formattedTimestamp = (zone: string) => {
    switch (zone) {
      case 'Local':
        return moment(timeVal)
          .tz(moment.tz.guess())
          .format('YYYY-MM-DD HH:mm:ss');
      case 'Relative':
        return moment(timeVal).fromNow();
      default:
        return moment(timeVal).utc().format('YYYY-MM-DD HH:mm:ss');
    }
  };

  if (!mounted) {
    return (
      <Label>
        <ProgressCircle variant='determinate_progress_circle' />
      </Label>
    );
  }

  return (
    <Label as='label' style={{ fontSize: fontSize }}>
      {formattedTimestamp(timezone)}
    </Label>
  );
}
