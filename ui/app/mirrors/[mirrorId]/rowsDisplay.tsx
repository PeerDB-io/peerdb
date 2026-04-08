'use client';
import { CDCRowCounts } from '@/grpc_generated/route';
import { useTheme } from '@/lib/AppTheme';
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
import { RowDisplayContainerStyle, RowsStatStyle } from './styles/row.styles';

export function RowDataFormatter(number: number) {
  return `${Intl.NumberFormat('en-US').format(number).toString()}`;
}

export default function RowsDisplay({
  totalRowsData,
}: {
  totalRowsData: CDCRowCounts;
}) {
  const theme = useTheme();
  const isDarkMode = theme.theme === 'dark';
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
      y: {
        grid: {
          color: isDarkMode ? 'rgba(255,255,255,0.1)' : 'rgba(0,0,0,0.1)',
        },
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
        backgroundColor: isDarkMode
          ? ['#115e44ff', '#ae8744ff', '#953535ff']
          : ['#10B981', '#F59E0B', '#EF4444'],
      },
    ],
  };

  return (
    <div style={RowDisplayContainerStyle}>
      <p style={RowsStatStyle}>
        {RowDataFormatter(totalRowsData.totalCount.valueOf())}
      </p>
      <p>Changes synced</p>

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
