import { PeerSlotResponse } from '@/grpc_generated/route';
import { Label } from '@/lib/Label';
import Link from 'next/link';

const getFlowName = (slotName: string) => {
  if (slotName.startsWith('peerflow_slot_')) {
    return slotName.slice(14);
  }
  return '';
};

export const getSlotData = async (peerName: string) => {
  try {
    const peerSlots: PeerSlotResponse = await fetch(
      `/api/peers/slots/peerData/${peerName}`,
      {
        cache: 'no-store',
      }
    )
      .then((res) => res.json())
      .catch((e) => {
        console.error('Error fetching slots:', e);
        return [];
      });

    const slotArray = peerSlots.slotData ?? [];
    // slots with 'peerflow_slot' should come first
    slotArray?.sort((slotA, slotB) => {
      if (
        slotA.slotName.startsWith('peerflow_slot') &&
        !slotB.slotName.startsWith('peerflow_slot')
      ) {
        return -1;
      } else if (
        !slotA.slotName.startsWith('peerflow_slot') &&
        slotB.slotName.startsWith('peerflow_slot')
      ) {
        return 1;
      } else {
        return 0;
      }
    });
    return slotArray;
  } catch (e) {
    console.error('Error fetching slots:', e);
    return [];
  }
};

export const SlotNameDisplay = ({ slotName }: { slotName: string }) => {
  const flowName = getFlowName(slotName);
  return flowName.length >= 1 ? (
    <Label
      as={Link}
      style={{
        cursor: 'pointer',
        textDecoration: 'underline',
        fontSize: 13,
        fontWeight: 'bold',
      }}
      href={`/mirrors/${flowName}`}
    >
      {slotName}
    </Label>
  ) : (
    <Label>{slotName}</Label>
  );
};

export const DurationDisplay = ({ duration }: { duration: number }) => {
  if (duration < 0) return 'N/A';
  return duration >= 3600
    ? `${Math.floor(duration / 3600)} hour(s) ${Math.floor(
        (duration % 3600) / 60
      )} minutes`
    : duration >= 60
      ? `${Math.floor(duration / 60)} minute(s) ${Math.floor(
          duration % 60
        )} seconds`
      : `${duration.toFixed(2)} seconds`;
};
