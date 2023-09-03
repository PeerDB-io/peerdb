import { ComponentProps } from 'react';
import {
  RadioButtonGroupRoot,
  RadioButtonIndicator,
  RadioButtonItem,
} from './RadioButtonGroup.styles';

type RadioButtonProps = ComponentProps<typeof RadioButtonItem>;

export function RadioButton({ ...radioButtonItemProps }: RadioButtonProps) {
  return (
    <RadioButtonItem {...radioButtonItemProps}>
      <RadioButtonIndicator />
    </RadioButtonItem>
  );
}

type RadioButtonGroupProps = ComponentProps<typeof RadioButtonGroupRoot>;

/**
 * Radio button group inputfield
 *
 * [Figma spec](https://www.figma.com/file/DBMDh1LNNvp9H99N9lZgJ7/PeerDB?type=design&node-id=1-1147&mode=design&t=b6qwokbWgkPCcgVS-4)
 */
export function RadioButtonGroup({
  children,
  ...radioButtonGroupProps
}: RadioButtonGroupProps) {
  return (
    <RadioButtonGroupRoot {...radioButtonGroupProps}>
      {children}
    </RadioButtonGroupRoot>
  );
}
