'use client';
import useTZStore from '@/app/globalstate/time';
import { Label } from '@/lib/Label';
import moment from 'moment-timezone';

const TimeLabel = ({
  timeVal,
  fontSize,
}: {
  timeVal: Date | string;
  fontSize?: number;
}) => {
  const timezone = useTZStore((state) => state.timezone);
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
