'use client';
import { PropsWithChildren } from 'react';
import { SlotWrapper } from './Slot.styles';

type SlotProps = PropsWithChildren<{
  className?: string;
}>;

/**
 * Slot component
 *
 * [Figma spec](https://www.figma.com/file/DBMDh1LNNvp9H99N9lZgJ7/PeerDB?type=design&node-id=9-1715&mode=design&t=RoHQa2CnPu35HWVa-4)
 */
export function Slot({ children, ...wrapperProps }: SlotProps) {
  return <SlotWrapper {...wrapperProps}>{children}</SlotWrapper>;
}
