'use client';
import SelectTheme from '@/app/styles/select';
import {
  formatGraphLabel,
  TimeAggregateTypes,
  timeOptions,
} from '@/app/utils/graph';
import {
  GetSlotLagHistoryRequest,
  GetSlotLagHistoryResponse,
} from '@/grpc_generated/route';
import { Label } from '@/lib/Label';
import { ProgressCircle } from '@/lib/ProgressCircle/ProgressCircle';
import { LineChart } from '@tremor/react';
import { useCallback, useEffect, useState } from 'react';
import ReactSelect from 'react-select';
import { useLocalStorage } from 'usehooks-ts';

type LagGraphProps = {
  peerName: string;
  slotNames: string[];
};

function LagGraph({ peerName, slotNames }: LagGraphProps) {
  const [mounted, setMounted] = useState(false);
  const [lagPoints, setLagPoints] = useState<
    { time: string; 'Lag in GB': number }[]
  >([]);
  const [defaultSlot, setDefaultSlot] = useLocalStorage('defaultSlot', '');
  const [selectedSlot, setSelectedSlot] = useState<string>(defaultSlot);
  const [loading, setLoading] = useState(false);
  let [timeSince, setTimeSince] = useState<TimeAggregateTypes>(
    TimeAggregateTypes.HOUR
  );
  const fetchLagPoints = useCallback(async () => {
    if (selectedSlot == '') {
      return;
    }

    setLoading(true);
    const pointsRes = await fetch(`/api/peers/slots/lag_history`, {
      method: 'POST',
      cache: 'no-store',
      body: JSON.stringify({
        peerName,
        slotName: selectedSlot,
        timeSince,
      } as GetSlotLagHistoryRequest),
    });
    const points: GetSlotLagHistoryResponse = await pointsRes.json();
    setLagPoints(
      points.data
        .sort((x, y) => x.updatedAt - y.updatedAt)
        .map((data) => ({
          time: formatGraphLabel(new Date(data.updatedAt!), timeSince),
          'Lag in GB': data.slotSize,
        }))
    );
    setLoading(false);
  }, [selectedSlot, timeSince, peerName]);

  const handleChange = (val: string) => {
    setDefaultSlot(val);
    setSelectedSlot(val);
  };

  useEffect(() => {
    setMounted(true);
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

        <ReactSelect
          id={timeSince}
          placeholder='Select a timeframe'
          options={timeOptions.filter((x) => !x.value.endsWith('min'))}
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
        <LineChart
          index='time'
          data={lagPoints}
          categories={['Lag in GB']}
          colors={['rose']}
          showXAxis={false}
        />
      )}
    </div>
  );
}

export default LagGraph;
