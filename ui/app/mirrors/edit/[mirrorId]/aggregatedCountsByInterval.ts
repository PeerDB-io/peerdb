type timestampType ={
  timestamp: string;
  count: number;
}

function aggregateCountsByInterval(timestamps: timestampType[], interval:string) {
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
      default:
        throw new Error('Invalid interval provided');
    }
  
    // Create an object to store the aggregated counts
    const aggregatedCounts :{ [key: string]: number } = {};
  
    // Iterate through the timestamps and populate the aggregatedCounts object
    for (let { timestamp, count } of timestamps) {
      timestamp = roundUpToNearest15Minutes(timestamp);
      const date = new Date(timestamp);
      const formattedTimestamp = formatTimestamp(date, timeUnit);
  
      if (!aggregatedCounts[formattedTimestamp]) {
        aggregatedCounts[formattedTimestamp] = 0;
      }
  
      aggregatedCounts[formattedTimestamp] += count;
    }

  
    
    // Create an array of intervals between the start and end timestamps
    const intervals = [];

    let currentTimestamp = new Date();

    if(interval === "15min"){
      currentTimestamp = roundUpToNearest15Minutes(currentTimestamp);
    }
    
    while (intervals.length < 30) {
      intervals.push(formatTimestamp(currentTimestamp, timeUnit));
      if (interval === 'hour') {
        currentTimestamp.setHours(currentTimestamp.getHours() - 1);
      } else if (interval === '15min') {
        currentTimestamp.setMinutes(currentTimestamp.getMinutes() - 15);
      } else if (interval === 'month') {
        currentTimestamp.setMonth(currentTimestamp.getMonth() - 1);
      } else if(interval === 'day'){
        currentTimestamp.setDate(currentTimestamp.getDate() - 1);
      }
    }
  
    // Populate the result array with intervals and counts
    const resultArray = intervals.map((interval) => [interval, aggregatedCounts[interval] || 0]);
    return resultArray;
  }

  function roundUpToNearest15Minutes(date:Date) {
    const minutes = date.getMinutes();
    const remainder = minutes % 15;
  
    if (remainder > 0) {
      // Round up to the nearest 15 minutes
      date.setMinutes(minutes + (15 - remainder));
    }
  
    // Reset seconds and milliseconds to zero to maintain the same time
    date.setSeconds(0);
    date.setMilliseconds(0);
  
    return date;
  }
  
  // Helper function to format a timestamp
  function formatTimestamp(date:Date, format:string) {
    const year = date.getFullYear();
    const month = padZero(date.getMonth() + 1); // Months are zero-based
    const day = padZero(date.getDate());
    const hour = padZero(date.getHours());
    const minutes = padZero(date.getMinutes());
  
    return format
      .replace('YYYY', year)
      .replace('MM', month)
      .replace('DD', day)
      .replace('HH', hour)
      .replace('mm', minutes);
  }
  
  // Helper function to pad single digits with leading zeros
  function padZero(number:number) {
    return number < 10 ? `0${number}` : `${number}`;
  }
  
export default aggregateCountsByInterval