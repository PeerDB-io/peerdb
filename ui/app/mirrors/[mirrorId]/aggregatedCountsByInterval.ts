import { TimeAggregateType } from '@/grpc_generated/route';
import { NearestMinutes, roundToNearestMinutes } from 'date-fns';
import moment from 'moment';

type timestampType = {
  timestamp: Date | string | undefined;
  count: number;
};

export default function aggregateCountsByInterval(
  timestamps: timestampType[],
  interval: TimeAggregateType
): [string, number][] {
  let timeUnit: string = 'YYYY-MM-DD HH:mm';
  let nearestMinutes: NearestMinutes = 1;
  switch (interval) {
    case TimeAggregateType.TIME_AGGREGATE_TYPE_FIVE_MIN:
      nearestMinutes = 5;
      break;
    case TimeAggregateType.TIME_AGGREGATE_TYPE_FIFTEEN_MIN:
      nearestMinutes = 15;
      break;
    case TimeAggregateType.TIME_AGGREGATE_TYPE_ONE_HOUR:
      timeUnit = 'YYYY-MM-DD HH:00:00';
      break;
    case TimeAggregateType.TIME_AGGREGATE_TYPE_ONE_DAY:
      timeUnit = 'YYYY-MM-DD';
      break;
    case TimeAggregateType.TIME_AGGREGATE_TYPE_ONE_MONTH:
      timeUnit = 'YYYY-MM';
      break;
    default:
      throw new Error('Invalid interval provided');
  }

  // Create an object to store the aggregated counts
  const aggregatedCounts: { [key: string]: number } = {};

  // Iterate through the timestamps and populate the aggregatedCounts object
  for (let { timestamp, count } of timestamps) {
    const currTs = new Date(timestamp ?? 0);
    const date = roundToNearestMinutes(currTs, { nearestTo: nearestMinutes });
    const formattedTimestamp = moment(date).format(timeUnit);

    if (!aggregatedCounts[formattedTimestamp]) {
      aggregatedCounts[formattedTimestamp] = 0;
    }

    aggregatedCounts[formattedTimestamp] += Number(count);
  }

  // Create an array of intervals between the start and end timestamps
  const intervals = [];

  let currentTimestamp = roundToNearestMinutes(new Date(), {
    nearestTo: nearestMinutes,
  });

  while (intervals.length < 30) {
    intervals.push(moment(currentTimestamp).format(timeUnit));
    if (interval === TimeAggregateType.TIME_AGGREGATE_TYPE_FIVE_MIN) {
      currentTimestamp.setMinutes(currentTimestamp.getMinutes() - 5);
    } else if (interval === TimeAggregateType.TIME_AGGREGATE_TYPE_FIFTEEN_MIN) {
      currentTimestamp.setMinutes(currentTimestamp.getMinutes() - 15);
    } else if (interval === TimeAggregateType.TIME_AGGREGATE_TYPE_ONE_HOUR) {
      currentTimestamp.setHours(currentTimestamp.getHours() - 1);
    } else if (interval === TimeAggregateType.TIME_AGGREGATE_TYPE_ONE_DAY) {
      currentTimestamp.setDate(currentTimestamp.getDate() - 1);
    } else if (interval === TimeAggregateType.TIME_AGGREGATE_TYPE_ONE_MONTH) {
      currentTimestamp.setMonth(currentTimestamp.getMonth() - 1);
    }
  }

  // Populate the result array with intervals and counts
  const resultArray: [string, number][] = intervals.map((interval) => [
    interval,
    Number(aggregatedCounts[interval]) || 0,
  ]);

  return resultArray;
}
