'use client';
import * as RadixToggleGroup from '@radix-ui/react-toggle-group';
import { PropsWithChildren } from 'react';
import { BaseToggleGroupRoot } from './ToggleGroup.styles';

type ToggleGroupProps = PropsWithChildren<{
  className?: string;
}> &
  Omit<RadixToggleGroup.ToggleGroupSingleProps, 'type'>;

/**
 * Toggle group component
 *
 * [Figma spec](https://www.figma.com/file/DBMDh1LNNvp9H99N9lZgJ7/PeerDB?type=design&node-id=1-1221&mode=design&t=fsqGSfRrNseMunYH-4)
 *
 * Based on the [Radix Toggle Group](https://www.radix-ui.com/primitives/docs/components/toggle-group) component
 */
export function ToggleGroup({ children, ...rootProps }: ToggleGroupProps) {
  return (
    <BaseToggleGroupRoot {...rootProps} type='single'>
      {children}
    </BaseToggleGroupRoot>
  );
}
