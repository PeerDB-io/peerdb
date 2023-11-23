'use client';
import { Label } from '@/lib/Label';
import { BarChart } from '@tremor/react';
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
      <BarChart
        className='mt-3'
        data={counts.map((count) => ({
          name: formatGraphLabel(new Date(count[0]), aggregateType),
          'Rows synced at a point in time': count[1],
        }))}
        index='name'
        categories={['Rows synced at a point in time']}
      />
    </div>
  );
}

function formatGraphLabel(date: Date, aggregateType: String): string {
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

export default CdcGraph;
