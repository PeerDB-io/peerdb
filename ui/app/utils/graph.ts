import moment from 'moment';

export const timeOptions = [
  { label: '1min', value: '1min' },
  { label: '5min', value: '5min' },
  { label: '15min', value: '15min' },
  { label: 'hour', value: 'hour' },
  { label: 'day', value: 'day' },
  { label: 'month', value: 'month' },
];

export function formatGraphLabel(date: Date, aggregateType: String): string {
  switch (aggregateType) {
    case '1min':
    case '5min':
    case '15min':
    case 'hour':
      return moment(date).format('MMM Do HH:mm');
    case 'day':
      return moment(date).format('MMM Do');
    case 'month':
      return moment(date).format('MMM yy');
    default:
      return 'Unknown aggregate type: ' + aggregateType;
  }
}
