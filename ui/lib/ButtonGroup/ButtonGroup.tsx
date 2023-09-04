'use client';
import { ComponentProps } from 'react';
import { BaseButtonGroup } from './ButtonGroup.styles';

type ButtonGroupProps = ComponentProps<typeof BaseButtonGroup>;

export function ButtonGroup({
  children,
  ...buttonGroupProps
}: ButtonGroupProps) {
  return <BaseButtonGroup {...buttonGroupProps}>{children}</BaseButtonGroup>;
}
