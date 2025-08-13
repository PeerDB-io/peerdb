import { TimeAggregateType } from '@/grpc_generated/route';
import moment from 'moment';

export const timeOptions = [
  { label: '5min', value: TimeAggregateType.TIME_AGGREGATE_TYPE_FIVE_MIN },
  { label: '15min', value: TimeAggregateType.TIME_AGGREGATE_TYPE_FIFTEEN_MIN },
  { label: '1 hour', value: TimeAggregateType.TIME_AGGREGATE_TYPE_ONE_HOUR },
  { label: '1 day', value: TimeAggregateType.TIME_AGGREGATE_TYPE_ONE_DAY },
  { label: '1 month', value: TimeAggregateType.TIME_AGGREGATE_TYPE_ONE_MONTH },
];

export function formatGraphLabel(
  date: Date,
  aggregateType: TimeAggregateType
): string {
  switch (aggregateType) {
    case TimeAggregateType.TIME_AGGREGATE_TYPE_FIVE_MIN:
    case TimeAggregateType.TIME_AGGREGATE_TYPE_FIFTEEN_MIN:
    case TimeAggregateType.TIME_AGGREGATE_TYPE_ONE_HOUR:
      return moment(date).format('MMM Do HH:mm');
    case TimeAggregateType.TIME_AGGREGATE_TYPE_ONE_DAY:
      return moment(date).format('MMM Do');
    case TimeAggregateType.TIME_AGGREGATE_TYPE_ONE_MONTH:
      return moment(date).format('MMM yy');
    default:
      throw new Error('Invalid aggregate type provided');
  }
}
