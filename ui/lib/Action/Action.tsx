import { ComponentProps } from 'react';
import { RenderSlot } from '../types';
import { renderSlotWith } from '../utils/renderSlotWith';
import { BaseAction } from './Action.styles';

type ActionProps = ComponentProps<'a'> & {
  icon?: RenderSlot;
  disabled?: boolean;
};

export function Action({
  icon,
  children,
  disabled,
  ...actionProps
}: ActionProps) {
  const Icon = renderSlotWith(icon);
  return (
    <BaseAction {...actionProps} data-disabled={disabled || undefined}>
      {Icon}
      {children}
    </BaseAction>
  );
}
