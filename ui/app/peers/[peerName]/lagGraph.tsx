'use client';
import { SlotLagPoint } from '@/app/dto/PeersDTO';
import { Label } from '@/lib/Label';
import { ProgressCircle } from '@/lib/ProgressCircle/ProgressCircle';
import { LineChart } from '@tremor/react';
import { useCallback, useEffect, useState } from 'react';
import ReactSelect from 'react-select';
import { useLocalStorage } from 'usehooks-ts';
function LagGraph({ slotNames }: { slotNames: string[] }) {
  const [lagPoints, setLagPoints] = useState<SlotLagPoint[]>([]);
  const [defaultSlot, setDefaultSlot] = useLocalStorage('defaultSlot', '');
  const [selectedSlot, setSelectedSlot] = useState<string>(defaultSlot);
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
      <LineChart
        index='time'
        data={lagPoints.map((point) => ({
          time: point.updatedAt,
          'Lag in MB': point.slotSize,
          Status: point.walStatus,
        }))}
        categories={['Lag in MB', 'Status']}
        colors={['rose', 'green']}
      />
    </div>
  );
}

export default LagGraph;
