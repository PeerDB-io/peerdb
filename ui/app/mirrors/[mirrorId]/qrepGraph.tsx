'use client';
import SelectTheme from '@/app/styles/select';
import {
  formatGraphLabel,
  TimeAggregateTypes,
  timeOptions,
} from '@/app/utils/graph';
import { PartitionStatus } from '@/grpc_generated/route';
import { Label } from '@/lib/Label';
import { BarChart } from '@tremor/react';
import { useEffect, useState } from 'react';
import ReactSelect from 'react-select';
import aggregateCountsByInterval from './aggregatedCountsByInterval';

type QRepGraphProps = {
  syncs: PartitionStatus[];
};

function QrepGraph({ syncs }: QRepGraphProps) {
  const [aggregateType, setAggregateType] = useState<TimeAggregateTypes>(
    TimeAggregateTypes.HOUR
  );
  const initialCount: [string, number][] = [];
  const [counts, setCounts] = useState(initialCount);

  useEffect(() => {
    const rows = syncs.map((sync) => ({
      timestamp: sync.startTime!,
      count: Number(sync.rowsInPartition) ?? 0,
    }));

    const counts = aggregateCountsByInterval(rows, aggregateType);
    setCounts(counts.slice(0, 29).reverse());
  }, [aggregateType, syncs]);

  return (
    <div>
      <div className='float-right'>
        <ReactSelect
          id={aggregateType}
          placeholder='Select a timeframe'
          options={timeOptions}
          defaultValue={timeOptions.at(3)}
          onChange={(val, _) => val && setAggregateType(val.value)}
          theme={SelectTheme}
        />
      </div>
      <div style={{ height: '3rem', marginTop: '1rem' }}>
        <Label variant='headline'>Partition sync history</Label>
      </div>
      <BarChart
        className='mt-3'
        data={counts.map((count) => ({
          name: formatGraphLabel(new Date(count[0]), aggregateType),
          'Rows synced at a point in time': Number(count[1]),
        }))}
        index='name'
        categories={['Rows synced at a point in time']}
      />
    </div>
  );
}

export default QrepGraph;
