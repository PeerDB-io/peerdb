'use client';
import { SlotLagPoint } from '@/app/dto/PeersDTO';
import aggregateCountsByInterval from '@/app/mirrors/edit/[mirrorId]/aggregatedCountsByInterval';
import { formatGraphLabel, timeOptions } from '@/app/utils/graph';
import { Label } from '@/lib/Label';
import { ProgressCircle } from '@/lib/ProgressCircle/ProgressCircle';
import { LineChart } from '@tremor/react';
import { useCallback, useEffect, useMemo, useState } from 'react';
import ReactSelect from 'react-select';
import { useLocalStorage } from 'usehooks-ts';

function LagGraph({ slotNames }: { slotNames: string[] }) {
  const [lagPoints, setLagPoints] = useState<SlotLagPoint[]>([]);
  const [defaultSlot, setDefaultSlot] = useLocalStorage('defaultSlot', '');
  const [selectedSlot, setSelectedSlot] = useState<string>(defaultSlot);
  let [aggregateType, setAggregateType] = useState('hour');
  const fetchLagPoints = useCallback(async () => {
    if (selectedSlot == '') {
      return;
    }
    const pointsRes = await fetch(`/api/peers/slots/${selectedSlot}`, {
      cache: 'no-store',
    });
    const points: SlotLagPoint[] = await pointsRes.json();
    setLagPoints(points);
  }, [selectedSlot]);

  const handleChange = (val: string) => {
    setDefaultSlot(val);
    setSelectedSlot(val);
  };

  const graphValues = useMemo(() => {
    let lagDataDot = aggregateCountsByInterval(
      lagPoints.map((point) => ({
        timestamp: point.updatedAt,
        count: parseInt(point.slotSize || '0', 10) || 0,
      })),
      aggregateType
    );
    lagDataDot = lagDataDot.slice(0, 29);
    lagDataDot = lagDataDot.reverse();
    return lagDataDot.map((data) => ({
      time: formatGraphLabel(new Date(data[0]), aggregateType),
      'Lag in MB': data[1],
    }));
  }, [lagPoints, aggregateType]);

  const [mounted, setMounted] = useState(false);
  useEffect(() => {
    setMounted(true);
  }, []);

  useEffect(() => {
    fetchLagPoints();
  }, [fetchLagPoints]);

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
          options={slotNames.map((slotName) => ({
            label: slotName,
            value: slotName,
          }))}
          onChange={(val, _) => val && handleChange(val.value)}
          defaultValue={{ value: selectedSlot, label: selectedSlot }}
        />

        <ReactSelect
          id={aggregateType}
          placeholder='Select a timeframe'
          options={timeOptions}
          defaultValue={{ label: 'hour', value: 'hour' }}
          onChange={(val, _) => val && setAggregateType(val.value)}
        />
      </div>
      <LineChart
        index='time'
        data={graphValues}
        categories={['Lag in MB']}
        colors={['rose']}
      />
    </div>
  );
}

export default LagGraph;
