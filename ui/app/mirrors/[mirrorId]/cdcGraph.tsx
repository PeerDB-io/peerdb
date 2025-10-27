'use client';
import SelectTheme from '@/app/styles/select';
import { formatGraphLabel, timeOptions } from '@/app/utils/graph';
import { TimeAggregateType } from '@/grpc_generated/route';
import { Label } from '@/lib/Label';
import {
  BarElement,
  CategoryScale,
  Chart as ChartJS,
  Legend,
  LinearScale,
  Title,
  Tooltip,
} from 'chart.js';
import { useState } from 'react';
import { Bar } from 'react-chartjs-2';
import ReactSelect from 'react-select';
import useSWR from 'swr';
type CdcGraphProps = { mirrorName: string };

export default function CdcGraph({ mirrorName }: CdcGraphProps) {
  const [aggregateType, setAggregateType] = useState<TimeAggregateType>(
    TimeAggregateType.TIME_AGGREGATE_TYPE_ONE_HOUR
  );

  ChartJS.register(
    BarElement,
    CategoryScale,
    LinearScale,
    Title,
    Tooltip,
    Legend
  );
  const chartOptions = {
    maintainAspectRatio: false,
    scales: {
      x: {
        grid: { display: false },
      },
    },
  };
  const fetcher = async ([mirrorName, aggregateType]: [
    string,
    TimeAggregateType,
  ]) => {
    const req = {
      flowJobName: mirrorName,
      aggregateType,
    };
    const res = await fetch('/api/v1/mirrors/cdc/graph', {
      method: 'POST',
      cache: 'no-store',
      body: JSON.stringify(req),
    });
    const data: { data: { time: number; rows: number }[] } = await res.json();

    const chartData = {
      labels: data.data.map(({ time }) =>
        formatGraphLabel(new Date(time), aggregateType)
      ),
      datasets: [
        {
          label: 'Rows synced at a point in time',
          data: data.data.map(({ rows }) => rows),
          backgroundColor: 'rgba(75, 192, 192, 0.2)',
          borderColor: 'rgba(75, 192, 192, 1)',
          borderWidth: 1,
        },
      ],
    };
    return chartData;
  };

  const { data, error } = useSWR([mirrorName, aggregateType], fetcher);

  return (
    <div>
      <div className='float-right'>
        <ReactSelect
          id={aggregateType.toString()}
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
      <div style={{ height: '320px' }}>
        {data && <Bar data={data} options={chartOptions} />}
      </div>
    </div>
  );
}
