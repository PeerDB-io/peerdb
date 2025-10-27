'use client';
import SelectTheme from '@/app/styles/select';
import { timeOptions } from '@/app/utils/graph';
import useLocalStorage from '@/app/utils/useLocalStorage';
import {
  GetSlotLagHistoryResponse,
  TimeAggregateType,
} from '@/grpc_generated/route';
import { Label } from '@/lib/Label';
import { ProgressCircle } from '@/lib/ProgressCircle/ProgressCircle';
import {
  CategoryScale,
  Chart as ChartJS,
  Legend,
  LinearScale,
  LineElement,
  PointElement,
  Title,
  Tooltip,
} from 'chart.js';
import moment from 'moment';
import { useCallback, useEffect, useState } from 'react';
import { Line } from 'react-chartjs-2';
import ReactSelect from 'react-select';
import { getSlotData } from './helpers';

type LagGraphProps = {
  peerName: string;
};

function parseLSN(lsn: string): number {
  if (!lsn) return 0;
  const [lsn1, lsn2] = lsn.split('/');
  const parsedLsn1 = parseInt(lsn1, 16);
  const parsedLsn2 = parseInt(lsn2, 16);
  if (isNaN(parsedLsn1) || isNaN(parsedLsn2)) return 0;
  return Number((BigInt(parsedLsn1) << BigInt(32)) | BigInt(parsedLsn2));
}

function stringifyTimeAggregateType(timeSince: TimeAggregateType): string {
  switch (timeSince) {
    case TimeAggregateType.TIME_AGGREGATE_TYPE_FIVE_MIN:
      return '5min';
    case TimeAggregateType.TIME_AGGREGATE_TYPE_FIFTEEN_MIN:
      return '15min';
    case TimeAggregateType.TIME_AGGREGATE_TYPE_ONE_HOUR:
      return '1hour';
    case TimeAggregateType.TIME_AGGREGATE_TYPE_ONE_DAY:
      return '1day';
    case TimeAggregateType.TIME_AGGREGATE_TYPE_ONE_MONTH:
      return '1month';
    default:
      return '1hour';
  }
}

export default function LagGraph({ peerName }: LagGraphProps) {
  const [slotNames, setSlotNames] = useState<string[]>([]);
  const [mounted, setMounted] = useState(false);
  const [lagPoints, setLagPoints] = useState<
    { time: string; 'Lag in GB': number }[]
  >([]);
  const [defaultSlot, setDefaultSlot] = useLocalStorage(
    `defaultSlot${peerName}`,
    ''
  );
  const [selectedSlot, setSelectedSlot] = useState<string>(defaultSlot);
  const [loading, setLoading] = useState(false);
  const [timeSince, setTimeSince] = useState<TimeAggregateType>(
    TimeAggregateType.TIME_AGGREGATE_TYPE_ONE_HOUR
  );
  const [showLsn, setShowLsn] = useState(false);

  const fetchSlotNames = useCallback(async () => {
    const slots = await getSlotData(peerName);
    setSlotNames(slots.map((slot) => slot.slotName));
  }, [peerName]);

  const fetchLagPoints = useCallback(async () => {
    if (selectedSlot == '') {
      return;
    }

    setLoading(true);
    const pointsRes = await fetch(`/api/v1/peers/slots/lag_history`, {
      method: 'POST',
      cache: 'no-store',
      body: JSON.stringify({
        peerName,
        slotName: selectedSlot,
        timeSince: stringifyTimeAggregateType(timeSince),
      }),
    });
    if (pointsRes.ok) {
      const points: GetSlotLagHistoryResponse = await pointsRes.json();
      setLagPoints(
        points.data
          .sort((x, y) => x.time - y.time)
          .map((data) => ({
            time: moment(data.time).format('MMM Do HH:mm'),
            'Lag in GB': data.size,
            redoLSN: parseLSN(data.redoLSN),
            restartLSN: parseLSN(data.restartLSN),
            confirmedLSN: parseLSN(data.confirmedLSN),
          }))
      );
    }
    setLoading(false);
  }, [selectedSlot, timeSince, peerName]);

  const handleChange = (val: string) => {
    setDefaultSlot(val);
    setSelectedSlot(val);
  };

  ChartJS.register(
    LineElement,
    CategoryScale,
    LinearScale,
    Title,
    Tooltip,
    Legend,
    PointElement
  );
  const chartOptions = {
    maintainAspectRatio: false,
    scales: {
      x: {
        display: false,
        grid: { display: false },
      },
    },
  };

  useEffect(() => {
    setMounted(true);
    fetchSlotNames();
    fetchLagPoints();
  }, [fetchLagPoints, fetchSlotNames]);
  const chartData = {
    labels: lagPoints.map((point) => point.time),
    datasets: [
      {
        label: 'Lag in GB',
        data: lagPoints.map((point) => point['Lag in GB']),
        backgroundColor: 'rgba(75, 192, 192, 0.2)',
        borderColor: 'rgba(75, 192, 192, 1)',
        borderWidth: 1,
      },
    ],
  };

  if (!mounted) {
    return (
      <Label>
        <ProgressCircle variant='determinate_progress_circle' />
      </Label>
    );
  }
  return (
    <div
      style={{
        display: 'flex',
        flexDirection: 'column',
        rowGap: '1rem',
        marginBottom: '2rem',
      }}
    >
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
        }}
      >
        <ReactSelect
          className='w-1/4'
          placeholder='Select a replication slot'
          options={
            slotNames.length === 0
              ? undefined
              : slotNames.map((slotName) => ({
                  label: slotName,
                  value: slotName,
                }))
          }
          onChange={(val, _) => val && handleChange(val.value)}
          defaultValue={
            selectedSlot
              ? { value: selectedSlot, label: selectedSlot }
              : undefined
          }
          theme={SelectTheme}
        />
        <input
          type='button'
          value={showLsn ? 'Show Lag' : 'Show LSN'}
          onClick={() => setShowLsn((val) => !val)}
        />
        <ReactSelect
          id={stringifyTimeAggregateType(timeSince)}
          placeholder='Select a timeframe'
          options={timeOptions.filter(
            (x) => !stringifyTimeAggregateType(x.value).endsWith('min')
          )}
          defaultValue={timeOptions.at(3)}
          onChange={(val, _) => val && setTimeSince(val.value)}
          theme={SelectTheme}
        />
      </div>
      {loading ? (
        <center>
          <Label>Updating slot graph</Label>
          <ProgressCircle variant='determinate_progress_circle' />
        </center>
      ) : (
        <div style={{ height: '20rem' }}>
          <Line data={chartData} options={chartOptions} />
        </div>
      )}
    </div>
  );
}
