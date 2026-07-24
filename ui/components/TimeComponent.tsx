'use client';
import useHydrated from '@/app/utils/useHydrated';
import useLocalStorage from '@/app/utils/useLocalStorage';
import { Label } from '@/lib/Label';
import { ProgressCircle } from '@/lib/ProgressCircle';
import moment from 'moment-timezone';

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
