import moment from 'moment';

export const enum TimeAggregateTypes {
  ONE_MIN = '1min',
  FIVE_MIN = '5min',
  FIFTEEN_MIN = '15min',
  HOUR = '1hour',
  DAY = '1day',
  MONTH = '1month',
}

export const timeOptions = [
  { label: '1min', value: TimeAggregateTypes.ONE_MIN },
  { label: '5min', value: TimeAggregateTypes.FIVE_MIN },
  { label: '15min', value: TimeAggregateTypes.FIFTEEN_MIN },
  { label: 'hour', value: TimeAggregateTypes.HOUR },
  { label: 'day', value: TimeAggregateTypes.DAY },
  { label: 'month', value: TimeAggregateTypes.MONTH },
];

export function formatGraphLabel(
  date: Date,
  aggregateType: TimeAggregateTypes
): string {
  switch (aggregateType) {
    case TimeAggregateTypes.ONE_MIN:
    case TimeAggregateTypes.FIVE_MIN:
    case TimeAggregateTypes.FIFTEEN_MIN:
    case TimeAggregateTypes.HOUR:
      return moment(date).format('MMM Do HH:mm');
    case TimeAggregateTypes.DAY:
      return moment(date).format('MMM Do');
    case TimeAggregateTypes.MONTH:
      return moment(date).format('MMM yy');
    default:
      throw new Error('Invalid aggregate type provided');
  }
}
