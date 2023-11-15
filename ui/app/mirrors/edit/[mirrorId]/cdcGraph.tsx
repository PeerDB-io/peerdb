import { Label } from '@/lib/Label';
import moment from 'moment';
import { useEffect, useState } from 'react';
import ReactSelect from 'react-select';
type SyncStatusRow = {
  batchId: number;
  startTime: Date;
  endTime: Date | null;
  numRows: number;
};

import aggregateCountsByInterval from './aggregatedCountsByInterval';

const aggregateTypeMap: { [key: string]: string } = {
  '15min': ' 15 mins',
  '5min': '5 mins',
  '1min': '1 min',
  hour: 'Hour',
  day: 'Day',
  month: 'Month',
  week: 'Week',
};

function CdcGraph({ syncs }: { syncs: SyncStatusRow[] }) {
  let [aggregateType, setAggregateType] = useState('hour');
  const initialCount: [string, number][] = [];
  let [counts, setCounts] = useState(initialCount);

  useEffect(() => {
    let rows = syncs.map((sync) => ({
      timestamp: sync.startTime,
      count: sync.numRows,
    }));

    let counts = aggregateCountsByInterval(rows, aggregateType);
    counts = counts.slice(0, 29);
    counts = counts.reverse();
    setCounts(counts);
  }, [aggregateType, syncs]);

  const timeOptions = [
    { label: '1min', value: '1min' },
    { label: '5min', value: '5min' },
    { label: '15min', value: '15min' },
    { label: 'hour', value: 'hour' },
    { label: 'day', value: 'day' },
    { label: 'week', value: 'week' },
    { label: 'month', value: 'month' },
  ];
  return (
    <div>
      <div className='float-right'>
        <ReactSelect
          id={aggregateType}
          placeholder='Select a timeframe'
          options={timeOptions}
          defaultValue={{ label: 'hour', value: 'hour' }}
          onChange={(val, _) => val && setAggregateType(val.value)}
        />
      </div>
      <div style={{ height: '3rem' }}>
        <Label variant='body'>Sync history</Label>
      </div>
      <div className='flex space-x-2 justify-left ml-2'>
        {counts.map((count, i) => (
          <GraphBar
            key={i}
            label={formatGraphLabel(new Date(count[0]), aggregateType)}
            count={count[1]}
          />
        ))}
      </div>
    </div>
  );
}

function formatGraphLabel(date: Date, aggregateType: String): string {
  switch (aggregateType) {
    case '15min':
      return moment(date).format('MMM Do HH:mm');
    case 'hour':
      return moment(date).format('MMM Do HH:mm');
    case 'day':
      return moment(date).format('MMM Do');
    case 'month':
      return moment(date).format('MMM yy');
    case 'week':
      return moment(date).format('MMM Do');
    case '5min':
      return moment(date).format('MMM Do HH:mm');
    case '1min':
      return moment(date).format('MMM Do HH:mm');
    default:
      return 'Unknown aggregate type: ' + aggregateType;
  }
}

type GraphBarProps = {
  count: number;
  label: string;
};

function GraphBar({ label, count }: GraphBarProps) {
  let color =
    count && count > 0 ? 'bg-positive-fill-normal' : 'bg-base-border-subtle';
  let classNames = `relative w-10 h-24 rounded  ${color}`;
  return (
    <div className={'group'}>
      <div className={classNames}>
        <div
          className='group-hover:opacity-100 transition-opacity bg-gray-800 px-1 text-sm text-gray-100 rounded-md absolute left-1/2 
        -translate-x-1/2 translate-y-full opacity-0 m-4 mx-auto w-28 z-10 text-center'
        >
          <div>{label}</div>
          <div>{numberWithCommas(count)}</div>
        </div>
      </div>
    </div>
  );
}

function numberWithCommas(x: number): string {
  return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',');
}

export default CdcGraph;
