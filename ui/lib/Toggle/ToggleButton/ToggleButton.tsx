import * as RadixToggle from '@radix-ui/react-toggle';
import { PropsWithChildren } from 'react';
import { BaseToggleRoot } from './ToggleButton.styles';

type ToggleButtonProps = PropsWithChildren<{ className?: string }> &
  RadixToggle.ToggleProps;

/**
 * Toggle button component
 *
 * [Figma spec](https://www.figma.com/file/DBMDh1LNNvp9H99N9lZgJ7/PeerDB?type=design&node-id=1-816&mode=design&t=fsqGSfRrNseMunYH-4)
 *
 * Based on the [Radix Toggle](https://www.radix-ui.com/primitives/docs/components/toggle) component
 */
export function ToggleButton({ children, ...rootProps }: ToggleButtonProps) {
  return <BaseToggleRoot {...rootProps}>{children}</BaseToggleRoot>;
}
