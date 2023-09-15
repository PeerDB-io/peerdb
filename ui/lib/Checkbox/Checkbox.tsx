'use client';
import { ComponentProps } from 'react';
import { Icon } from '../Icon';
import { CheckboxIndicator, CheckboxRoot } from './Checkbox.styles';

type CheckboxProps = ComponentProps<typeof CheckboxRoot> & {
  /** Set a variant of checkmark to display */
  variant?: 'check' | 'mixed';
};

/**
 * Checkbox inputfield
 *
 * [Figma spec](https://www.figma.com/file/DBMDh1LNNvp9H99N9lZgJ7/PeerDB?type=design&node-id=1-473&mode=design&t=b6qwokbWgkPCcgVS-4)
 */
export function Checkbox({
  variant = 'check',
  ...checkboxProps
}: CheckboxProps) {
  return (
    <CheckboxRoot {...checkboxProps}>
      <CheckboxIndicator>
        <Icon
          name={variant === 'check' ? 'check' : 'check_indeterminate_small'}
        />
      </CheckboxIndicator>
    </CheckboxRoot>
  );
}
