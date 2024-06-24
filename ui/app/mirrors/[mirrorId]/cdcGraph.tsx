'use client';
import { SyncStatusRow } from '@/app/dto/MirrorsDTO';
import SelectTheme from '@/app/styles/select';
import {
  formatGraphLabel,
  TimeAggregateTypes,
  timeOptions,
} from '@/app/utils/graph';
import { Label } from '@/lib/Label';
import { BarChart } from '@tremor/react';
import { useMemo, useState } from 'react';
import ReactSelect from 'react-select';
import aggregateCountsByInterval from './aggregatedCountsByInterval';

function CdcGraph({ syncs }: { syncs: SyncStatusRow[] }) {
  let [aggregateType, setAggregateType] = useState<TimeAggregateTypes>(
    TimeAggregateTypes.HOUR
  );

  const graphValues = useMemo(() => {
    const rows = syncs.map((sync) => ({
      timestamp: sync.endTime,
      count: sync.numRows,
    }));
    let timedRowCounts = aggregateCountsByInterval(rows, aggregateType);
    timedRowCounts = timedRowCounts.slice(0, 29);
    timedRowCounts = timedRowCounts.reverse();
    return timedRowCounts.map((count) => ({
      name: formatGraphLabel(new Date(count[0]), aggregateType),
      'Rows synced at a point in time': count[1],
    }));
  }, [syncs, aggregateType]);

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
      <div style={{ height: '3rem' }}>
        <Label variant='headline'>Sync history</Label>
      </div>
      <BarChart
        className='mt-3'
        data={graphValues}
        index='name'
        categories={['Rows synced at a point in time']}
      />
    </div>
  );
}

export default CdcGraph;
