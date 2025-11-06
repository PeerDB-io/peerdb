'use client';
import SelectTheme from '@/app/styles/select';
import { formatGraphLabel, timeOptions } from '@/app/utils/graph';
import { PartitionStatus, TimeAggregateType } from '@/grpc_generated/route';
import { useTheme } from '@/lib/AppTheme';
import { Label } from '@/lib/Label';
import { Chart as ChartJS, ChartOptions } from 'chart.js';
import { useEffect, useMemo, useRef, useState } from 'react';
import { Bar } from 'react-chartjs-2';
import ReactSelect from 'react-select';
import aggregateCountsByInterval from './aggregatedCountsByInterval';

type QRepGraphProps = {
  syncs: PartitionStatus[];
};

function QrepGraph({ syncs }: QRepGraphProps) {
  const theme = useTheme();
  const isDarkMode = theme.theme === 'dark';
  const [aggregateType, setAggregateType] = useState<TimeAggregateType>(
    TimeAggregateType.TIME_AGGREGATE_TYPE_ONE_HOUR
  );

  const chartRef = useRef<ChartJS<'bar'> | null>(null);

  useEffect(() => {
    return () => {
      if (chartRef.current) {
        chartRef.current.destroy();
        chartRef.current = null;
      }
    };
  }, []);

  const chartOptions: ChartOptions<'bar'> = {
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

  const counts = useMemo(() => {
    const rows = syncs.map((sync) => ({
      timestamp: sync.startTime!,
      count: Number(sync.rowsInPartition) ?? 0,
    }));

    const aggregatedCounts = aggregateCountsByInterval(rows, aggregateType);
    return aggregatedCounts.slice(0, 29).reverse();
  }, [aggregateType, syncs]);

  const qrepData = {
    labels: counts.map((count) =>
      formatGraphLabel(new Date(count[0]), aggregateType)
    ),
    datasets: [
      {
        label: 'Rows synced at a point in time',
        data: counts.map((count) => Number(count[1])),
        backgroundColor: 'rgba(75, 192, 192, 0.2)',
        borderColor: 'rgba(75, 192, 192, 1)',
        borderWidth: 1,
      },
    ],
  };

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
      <div style={{ height: '3rem', marginTop: '1rem' }}>
        <Label variant='headline'>Partition sync history</Label>
      </div>

      <div style={{ height: '20rem' }}>
        {qrepData && (
          <Bar
            ref={(chart) => {
              chartRef.current = chart ?? null;
            }}
            data={qrepData}
            options={chartOptions}
          />
        )}
      </div>
    </div>
  );
}

export default QrepGraph;
