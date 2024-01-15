import moment from 'moment';

type timestampType = {
  timestamp: Date;
  count: number;
};

function aggregateCountsByInterval(
  timestamps: timestampType[],
  interval: string
): [string, number][] {
  let timeUnit;
  switch (interval) {
    case 'hour':
      timeUnit = 'YYYY-MM-DD HH:00:00';
      break;
    case '15min':
      timeUnit = 'YYYY-MM-DD HH:mm';
      break;
    case 'month':
      timeUnit = 'YYYY-MM';
      break;
    case 'day':
      timeUnit = 'YYYY-MM-DD';
      break;
    case '1min':
    case '5min':
      timeUnit = 'YYYY-MM-DD HH:mm';
      break;
    default:
      throw new Error('Invalid interval provided');
  }

  // Create an object to store the aggregated counts
  const aggregatedCounts: { [key: string]: number } = {};

  // Iterate through the timestamps and populate the aggregatedCounts object
  for (let { timestamp, count } of timestamps) {
    let N = 1;
    if (interval === '1min') {
      N = 1;
    } else if (interval === '5min') {
      N = 5;
    } else if (interval === '15min') {
      N = 15;
    }

    const currTs = new Date(timestamp);
    const date = roundUpToNearestNMinutes(currTs, N);
    const formattedTimestamp = moment(date).format(timeUnit);

    if (!aggregatedCounts[formattedTimestamp]) {
      aggregatedCounts[formattedTimestamp] = 0;
    }

    aggregatedCounts[formattedTimestamp] += count;
  }

  // Create an array of intervals between the start and end timestamps
  const intervals = [];

  let currentTimestamp = new Date();

  if (interval === '15min') {
    currentTimestamp = roundUpToNearestNMinutes(currentTimestamp, 15);
  }
  if (interval === '5min') {
    currentTimestamp = roundUpToNearestNMinutes(currentTimestamp, 5);
  }

  while (intervals.length < 30) {
    intervals.push(moment(currentTimestamp).format(timeUnit));
    if (interval === 'hour') {
      currentTimestamp.setHours(currentTimestamp.getHours() - 1);
    } else if (interval === '15min') {
      currentTimestamp.setMinutes(currentTimestamp.getMinutes() - 15);
    } else if (interval === '1min') {
      currentTimestamp.setMinutes(currentTimestamp.getMinutes() - 1);
    } else if (interval === '5min') {
      currentTimestamp.setMinutes(currentTimestamp.getMinutes() - 5);
    } else if (interval === 'month') {
      currentTimestamp.setMonth(currentTimestamp.getMonth() - 1);
    } else if (interval === 'day') {
      currentTimestamp.setDate(currentTimestamp.getDate() - 1);
    }
  }

  // Populate the result array with intervals and counts
  const resultArray: [string, number][] = intervals.map((interval) => [
    interval,
    aggregatedCounts[interval] || 0,
  ]);

  return resultArray;
}

function roundUpToNearestNMinutes(date: Date, N: number) {
  const minutes = date.getMinutes();
  const remainder = minutes % N;

  if (remainder > 0) {
    // Round up to the nearest N minutes
    date.setMinutes(minutes + (N - remainder));
  }

  // Reset seconds and milliseconds to zero to maintain the same time
  date.setSeconds(0);
  date.setMilliseconds(0);

  return date;
}

export default aggregateCountsByInterval;
