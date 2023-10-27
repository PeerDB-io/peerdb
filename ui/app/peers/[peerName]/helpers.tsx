import { Label } from '@/lib/Label';
import Link from 'next/link';

const getFlowName = (slotName: string) => {
  if (slotName.startsWith('peerflow_slot_')) {
    return slotName.slice(14);
  }
  return '';
};

export const SlotNameDisplay = ({ slotName }: { slotName: string }) => {
  const flowName = getFlowName(slotName);
  return flowName.length >= 1 ? (
    <Label
      as={Link}
      style={{
        color: 'darkblue',
        cursor: 'pointer',
        textDecoration: 'underline',
      }}
      href={`/mirrors/edit/${flowName}`}
    >
      {slotName}
    </Label>
  ) : (
    <Label>{slotName}</Label>
  );
};

export const DurationDisplay = ({ duration }: { duration: number }) => {
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
