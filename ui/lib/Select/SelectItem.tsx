'use client';
import * as RadixSelect from '@radix-ui/react-select';
import type { PropsWithChildren } from 'react';
import { Icon } from '../Icon';
import { StyledIndicator, StyledItem } from './SelectItem.styles';

type SelectItemProps = PropsWithChildren<RadixSelect.SelectItemProps>;
export function SelectItem({ children, ...selectItemProps }: SelectItemProps) {
  return (
    <StyledItem {...selectItemProps}>
      <StyledIndicator>
        <Icon name='check' />
      </StyledIndicator>
      <RadixSelect.ItemText>{children}</RadixSelect.ItemText>
    </StyledItem>
  );
}
