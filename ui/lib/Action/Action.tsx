'use client';

import { ComponentProps } from 'react';
import { RenderObject } from '../types';
import { renderObjectWith } from '../utils/renderObjectWith';
import { BaseAction } from './Action.styles';

type ActionProps = ComponentProps<'a'> & {
  icon?: RenderObject;
  disabled?: boolean;
};

export function Action({
  icon,
  children,
  disabled,
  ref: _ref,
  ...actionProps
}: ActionProps) {
  const Icon = renderObjectWith(icon);
  return (
    <BaseAction {...actionProps} data-disabled={disabled || undefined}>
      {Icon}
      {children}
    </BaseAction>
  );
}
