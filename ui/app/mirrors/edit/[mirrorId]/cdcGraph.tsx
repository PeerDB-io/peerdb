import { Label } from '@/lib/Label';
import moment from 'moment';
import { useEffect, useState } from 'react';

type SyncStatusRow = {
  batchId: number;
  startTime: Date;
  endTime: Date | null;
  numRows: number;
};

import aggregateCountsByInterval from './aggregatedCountsByInterval';

const aggregateTypeMap: { [key: string]: string } = {
  '15min': ' 15 mins',
  hour: 'Hour',
  day: 'Day',
  month: 'Month',
};

function CdcGraph({ syncs }: { syncs: SyncStatusRow[] }) {
  let [aggregateType, setAggregateType] = useState('hour');
  const initialCount: [string, number][] = [];
  let [counts, setCounts] = useState(initialCount);

  let rows = syncs.map((sync) => ({
    timestamp: sync.startTime,
    count: sync.numRows,
  }));

  useEffect(() => {
    let counts = aggregateCountsByInterval(rows, aggregateType);
    counts = counts.slice(0, 29);
    counts = counts.reverse();
    setCounts(counts);
  }, [aggregateType, rows]);

  return (
    <div>
      <div className='float-right'>
        {['15min', 'hour', 'day', 'month'].map((type) => {
          return (
            <FilterButton
              key={type}
              aggregateType={type}
              selectedAggregateType={aggregateType}
              setAggregateType={setAggregateType}
            />
          );
        })}
      </div>
      <div>
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

type filterButtonProps = {
  aggregateType: string;
  selectedAggregateType: string;
  setAggregateType: Function;
};
function FilterButton({
  aggregateType,
  selectedAggregateType,
  setAggregateType,
}: filterButtonProps): React.ReactNode {
  return (
    <button
      className={
        aggregateType === selectedAggregateType
          ? 'bg-gray-200 px-1 mx-1 rounded-md'
          : 'px-1 mx-1'
      }
      onClick={() => setAggregateType(aggregateType)}
    >
      {aggregateTypeMap[aggregateType]}
    </button>
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
