'use client';
import SelectTheme from '@/app/styles/select';
import { formatGraphLabel, timeOptions } from '@/app/utils/graph';
import { PartitionStatus, TimeAggregateType } from '@/grpc_generated/route';
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
import { useEffect, useMemo, useRef, useState } from 'react';
import { Bar } from 'react-chartjs-2';
import ReactSelect from 'react-select';
import aggregateCountsByInterval from './aggregatedCountsByInterval';

type QRepGraphProps = {
  syncs: PartitionStatus[];
};

function QrepGraph({ syncs }: QRepGraphProps) {
  const [aggregateType, setAggregateType] = useState<TimeAggregateType>(
    TimeAggregateType.TIME_AGGREGATE_TYPE_ONE_HOUR
  );

  // Ref to track the chart instance
  const chartRef = useRef<ChartJS<'bar'> | null>(null);

  // Register Chart.js components only once
  useEffect(() => {
    ChartJS.register(
      BarElement,
      CategoryScale,
      LinearScale,
      Title,
      Tooltip,
      Legend
    );

    // Cleanup function to destroy chart on unmount
    return () => {
      if (chartRef.current) {
        chartRef.current.destroy();
        chartRef.current = null;
      }
    };
  }, []);

  const chartOptions: ChartOptions<'bar'> = {
    maintainAspectRatio: false,
    responsive: true,
    // Add animation configuration to reduce memory usage
    animation: false,
    // Optimize performance
    interaction: {
      intersect: false,
    },
    scales: {
      x: {
        grid: { display: false },
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
