'use client';
import * as RadixSelect from '@radix-ui/react-select';
import { PropsWithChildren } from 'react';
import { SelectContent } from './SelectMenu.styles';

type SelectMenuProps = PropsWithChildren<RadixSelect.SelectContentProps>;
export function SelectMenu({ children, ...contentProps }: SelectMenuProps) {
  return (
    <SelectContent {...contentProps}>
      <RadixSelect.Viewport>{children}</RadixSelect.Viewport>
    </SelectContent>
  );
}
