'use client';
import { RenderObject } from '../types';
import { renderObjectWith } from '../utils/renderObjectWith';
import type { BaseLayoutProps } from './BaseLayout';
import { LayoutWrapper, StyledFlexRow } from './BaseLayout.styles';

export type BaseLayoutColumnProps = BaseLayoutProps & {
  actionSlot?: RenderObject;
  suffix?: RenderObject;
};

export function BaseLayoutColumn({
  label,
  action,
  description,
  instruction,
  actionSlot,
  suffix,
  ...wrapperProps
}: BaseLayoutColumnProps) {
  const Label = renderObjectWith(label, {
    variant: 'body',
    className: 'layout-label',
  });

  const Description = renderObjectWith(description, {
    variant: 'subheadline',
    className: 'layout-description',
  });

  const Suffix = renderObjectWith(suffix, {
    variant: 'subheadline',
    className: 'layout-suffix',
  });

  const Instruction = renderObjectWith(instruction, {
    variant: 'subheadline',
    className: 'layout-instruction',
  });

  const Action = renderObjectWith(action, {
    className: 'layout-action layout-action--flex-auto',
  });

  const ActionSlot = renderObjectWith(actionSlot, {
    className: 'layout-action-slot',
  });

  return (
    <LayoutWrapper direction='column' {...wrapperProps}>
      <StyledFlexRow>
        {Label}
        {Suffix}
      </StyledFlexRow>
      <StyledFlexRow>
        {Action}
        {ActionSlot}
      </StyledFlexRow>
      {Instruction}
      {Description}
    </LayoutWrapper>
  );
}
