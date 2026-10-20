'use client';
import { useSelectTheme } from '@/app/styles/select';
import { formatGraphLabel, timeOptions } from '@/app/utils/graph';
import { LagGraphResponse, TimeAggregateType } from '@/grpc_generated/route';
import { useTheme } from '@/lib/AppTheme';
import { BarLoader } from '@/lib/BarLoader';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { Chart as ChartJS, ChartOptions } from 'chart.js';
import { useEffect, useRef, useState } from 'react';
import { Line } from 'react-chartjs-2';
import ReactSelect from 'react-select';
import useSWR from 'swr';

const toSeconds = (ms: number | null | undefined) =>
  ms == null ? null : ms / 1000;

const lagTimeOptions = [
  { label: '1 min', value: TimeAggregateType.TIME_AGGREGATE_TYPE_ONE_MIN },
  ...timeOptions,
];

type LagGraphProps = { mirrorName: string };

export default function LagGraph({ mirrorName }: LagGraphProps) {
  const selectTheme = useSelectTheme();
  const [aggregateType, setAggregateType] = useState<TimeAggregateType>(
    TimeAggregateType.TIME_AGGREGATE_TYPE_ONE_MIN
  );

  const chartRef = useRef<ChartJS<'line'> | null>(null);

  useEffect(() => {
    return () => {
      if (chartRef.current) {
        chartRef.current.destroy();
        chartRef.current = null;
      }
    };
  }, []);

  const theme = useTheme();
  const isDarkMode = theme.theme === 'dark';

  const chartOptions: ChartOptions<'line'> = {
    interaction: {
      mode: 'index',
      intersect: false,
    },
    spanGaps: false,
    scales: {
      x: {
        grid: { display: false },
      },
      y: {
        beginAtZero: true,
        title: { display: true, text: 'seconds' },
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
    const res = await fetch('/api/v1/mirrors/cdc/lag_graph', {
      method: 'POST',
      cache: 'no-store',
      body: JSON.stringify({ flowJobName: mirrorName, aggregateType }),
    });
    const data: LagGraphResponse = await res.json();

    return {
      labels: data.data.map(({ time }) =>
        formatGraphLabel(new Date(time), aggregateType)
      ),
      datasets: [
        {
          label: 'Source lag',
          data: data.data.map(({ sourceLagMs }) => toSeconds(sourceLagMs)),
          borderColor: 'rgba(54, 162, 235, 1)',
          backgroundColor: 'rgba(54, 162, 235, 0.2)',
        },
        {
          label: 'Destination lag',
          data: data.data.map(({ destinationLagMs }) =>
            toSeconds(destinationLagMs)
          ),
          borderColor: 'rgba(255, 159, 64, 1)',
          backgroundColor: 'rgba(255, 159, 64, 0.2)',
        },
        {
          label: 'End-to-end lag',
          data: data.data.map(({ e2eLagMs }) => toSeconds(e2eLagMs)),
          borderColor: 'rgba(153, 102, 255, 1)',
          backgroundColor: 'rgba(153, 102, 255, 0.2)',
        },
      ],
    };
  };

  const { data, isLoading, error } = useSWR(
    [mirrorName, aggregateType],
    fetcher,
    {
      refreshInterval: 30_000,
      revalidateOnFocus: false,
      revalidateOnReconnect: false,
    }
  );

  return (
    <div>
      <div style={{ float: 'right' }}>
        <ReactSelect
          id='lag-timeframe'
          placeholder='Select a timeframe'
          options={lagTimeOptions}
          defaultValue={lagTimeOptions.at(0)}
          onChange={(val, _) => val && setAggregateType(val.value)}
          theme={selectTheme}
        />
      </div>
      <div style={{ height: '3rem' }}>
        <Label variant='headline'>Replication lag</Label>
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
        {isLoading && (
          <div
            style={{
              display: 'flex',
              rowGap: '1rem',
              flexDirection: 'column',
              alignItems: 'center',
            }}
          >
            Loading lag data...
            <BarLoader />
          </div>
        )}
        {error && (
          <div
            style={{
              display: 'flex',
              alignItems: 'center',
              columnGap: '0.5rem',
            }}
          >
            <Icon name='error' /> Failed to load replication lag
          </div>
        )}
        {data && !isLoading && !error && (
          <Line
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
