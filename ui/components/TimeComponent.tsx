'use client';
import { Label } from '@/lib/Label';
import moment from 'moment-timezone';
import { useLocalStorage } from 'usehooks-ts';

const TimeLabel = ({
  timeVal,
  fontSize,
}: {
  timeVal: Date | string;
  fontSize?: number;
}) => {
  const [timezone] = useLocalStorage('timezone-ui', 'UTC'); // ['UTC', 'Local', 'Relative']
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
  return (
    <Label as='label' style={{ fontSize: fontSize }}>
      {formattedTimestamp(timezone)}
    </Label>
  );
};

export default TimeLabel;
