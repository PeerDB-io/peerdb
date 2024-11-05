'use client';
import SelectTheme from '@/app/styles/select';
import {
  formatGraphLabel,
  TimeAggregateTypes,
  timeOptions,
} from '@/app/utils/graph';
import { Label } from '@/lib/Label';
import { BarChart } from '@tremor/react';
import { useEffect, useState } from 'react';
import ReactSelect from 'react-select';

type CdcGraphProps = { mirrorName: string };

export default function CdcGraph({ mirrorName }: CdcGraphProps) {
  const [aggregateType, setAggregateType] = useState<TimeAggregateTypes>(
    TimeAggregateTypes.HOUR
  );
  const [graphValues, setGraphValues] = useState<
    { name: string; 'Rows synced at a point in time': number }[]
  >([]);

  useEffect(() => {
    const fetchData = async () => {
      const req: any = {
        flowJobName: mirrorName,
        aggregateType,
      };

      const res = await fetch('/api/v1/mirrors/cdc/graph', {
        method: 'POST',
        cache: 'no-store',
        body: JSON.stringify(req),
      });
      const data: { data: { time: number; rows: number }[] } = await res.json();
      setGraphValues(
        data.data.map(({ time, rows }) => ({
          name: formatGraphLabel(new Date(time), aggregateType),
          'Rows synced at a point in time': Number(rows),
        }))
      );
    };

    fetchData();
  }, [mirrorName, aggregateType]);

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
