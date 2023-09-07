'use client';
// your-select.jsx
import * as RadixSelect from '@radix-ui/react-select';
import { PropsWithChildren } from 'react';
import { Icon } from '../Icon';
import { StyledSelectIcon, StyledSelectTrigger } from './Select.styles';
import { SelectMenu } from './SelectMenu';

type SelectProps = PropsWithChildren<RadixSelect.SelectProps> & {
  placeholder?: string;
  id?: string;
  className?: string;
};
export function Select({
  placeholder,
  children,
  id,
  className,
  ...selectRootProps
}: SelectProps) {
  return (
    <RadixSelect.Root {...selectRootProps}>
      <StyledSelectTrigger id={id} className={className}>
        <RadixSelect.Value placeholder={placeholder} />
        <StyledSelectIcon asChild>
          <Icon name='expand_more' />
        </StyledSelectIcon>
      </StyledSelectTrigger>
      <RadixSelect.Portal>
        <SelectMenu position='popper'>{children}</SelectMenu>
      </RadixSelect.Portal>
    </RadixSelect.Root>
  );
}
