'use client';
import { CDCRowCounts } from '@/grpc_generated/route';
import { Button } from '@/lib/Button';
import { Icon } from '@/lib/Icon';
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
import { useState } from 'react';
import { Bar } from 'react-chartjs-2';
import { RowDisplayContainerStyle } from './styles/rowStyles';

export function RowDataFormatter(number: number) {
  return `${Intl.NumberFormat('en-US').format(number).toString()}`;
}

export default function RowsDisplay({
  totalRowsData,
}: {
  totalRowsData: CDCRowCounts;
}) {
  ChartJS.register(
    BarElement,
    CategoryScale,
    LinearScale,
    Title,
    Tooltip,
    Legend
  );
  const chartOptions: ChartOptions<'bar'> = {
    indexAxis: 'y',
    plugins: {
      legend: { display: false },
    },
    scales: {
      x: {
        beginAtZero: true,
        grid: { display: false },
      },
    },
    layout: {
      padding: {
        left: 0,
        right: 24,
        top: 8,
        bottom: 8,
      },
    },
    animation: { duration: 600 },
  };
  const [show, setShow] = useState(false);
  const rowData = {
    labels: ['Inserts', 'Updates', 'Deletes'],
    datasets: [
      {
        data: [
          totalRowsData.insertsCount,
          totalRowsData.updatesCount,
          totalRowsData.deletesCount,
        ],
        backgroundColor: ['#10B981', '#F59E0B', '#EF4444'],
      },
    ],
  };

  return (
    <div style={RowDisplayContainerStyle}>
      <h4 className='text-gray-600 dark:text-gray-300'>Rows Synced</h4>
      <p className='text-gray-900 dark:text-white font-semibold'>
        {RowDataFormatter(totalRowsData.totalCount.valueOf())}
      </p>
      <Button
        aria-label='icon-button'
        onClick={() => setShow((prev) => !prev)}
        variant='normal'
      >
        <Icon name={show ? 'arrow_drop_up' : 'arrow_drop_down'} />
      </Button>
      {show && (
        <div
          style={{ width: '30%', marginTop: '1.5rem' }}
          className='[&_p]:text-gray-700 dark:[&_p]:text-gray-300'
        >
          <Bar data={rowData} options={chartOptions} />
        </div>
      )}
    </div>
  );
}
