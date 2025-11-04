'use client';
import SelectTheme from '@/app/styles/select';
import { formatGraphLabel, timeOptions } from '@/app/utils/graph';
import { TimeAggregateType } from '@/grpc_generated/route';
import { useTheme } from '@/lib/AppTheme';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import {
  BarElement,
  CategoryScale,
  Chart as ChartJS,
  ChartOptions,
  Legend,
  LinearScale,
  Title,
  Tooltip,
} from 'chart.js';
import { useEffect, useRef, useState } from 'react';
import { Bar } from 'react-chartjs-2';
import ReactSelect from 'react-select';
import { BarLoader } from 'react-spinners';
import useSWR from 'swr';

const CdcSyncingLoader = () => {
  return (
    <div
      style={{
        display: 'flex',
        rowGap: '1rem',
        flexDirection: 'column',
        alignItems: 'center',
      }}
    >
      Loading sync data...
      <BarLoader />
    </div>
  );
};

const CdcSyncHistoryError = () => {
  return (
    <div style={{ display: 'flex', alignItems: 'center', columnGap: '0.5rem' }}>
      <Icon name='error' /> Failed to load sync history
    </div>
  );
};

type CdcGraphProps = { mirrorName: string };

export default function CdcGraph({ mirrorName }: CdcGraphProps) {
  const [aggregateType, setAggregateType] = useState<TimeAggregateType>(
    TimeAggregateType.TIME_AGGREGATE_TYPE_ONE_HOUR
  );

  const chartRef = useRef<ChartJS<'bar'> | null>(null);

  useEffect(() => {
    ChartJS.register(
      BarElement,
      CategoryScale,
      LinearScale,
      Title,
      Tooltip,
      Legend
    );

    return () => {
      if (chartRef.current) {
        chartRef.current.destroy();
        chartRef.current = null;
      }
    };
  }, []);

  const theme = useTheme();
  const isDarkMode = theme.theme === 'dark';

  const chartOptions: ChartOptions<'bar'> = {
    maintainAspectRatio: false,
    responsive: true,
    animation: false,
    interaction: {
      intersect: false,
    },
    scales: {
      x: {
        grid: { display: false },
      },
      y: {
        grid: {
          color: isDarkMode ? '#333333' : '#e5e7eb',
        },
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

  const { data, isLoading, error } = useSWR(
    [mirrorName, aggregateType],
    fetcher,
    {
      // Add SWR configuration to help with memory management
      revalidateOnFocus: false,
      revalidateOnReconnect: false,
    }
  );

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
      <div
        style={{
          height: '320px',
          alignItems: 'center',
          justifyContent: 'center',
          display: 'flex',
          width: '100%',
        }}
      >
        {isLoading && <CdcSyncingLoader />}
        {error && <CdcSyncHistoryError />}
        {data && !isLoading && !error && (
          <Bar
            ref={(chart) => {
              chartRef.current = chart ?? null;
            }}
            data={data}
            options={chartOptions}
          />
        )}
      </div>
    </div>
  );
}
