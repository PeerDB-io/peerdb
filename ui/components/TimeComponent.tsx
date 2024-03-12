'use client';
import { Label } from '@/lib/Label';
import { ProgressCircle } from '@/lib/ProgressCircle';
import moment from 'moment-timezone';
import { useEffect, useState } from 'react';
import { useLocalStorage } from 'usehooks-ts';

const TimeLabel = ({
  timeVal,
  fontSize,
}: {
  timeVal: Date | string;
  fontSize?: number;
}) => {
  const [timezone] = useLocalStorage('timezone-ui', 'UTC'); // ['UTC', 'Local', 'Relative']
  const [mounted, setMounted] = useState(false);
  useEffect(() => {
    setMounted(true);
  }, []);
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
};

export default TimeLabel;
