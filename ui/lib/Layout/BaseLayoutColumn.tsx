import { RenderSlot } from '../types';
import { renderSlotWith } from '../utils/renderSlotWith';
import type { BaseLayoutProps } from './BaseLayout';
import { LayoutWrapper, StyledFlexRow } from './BaseLayout.styles';

export type BaseLayoutColumnProps = BaseLayoutProps & {
  actionSlot?: RenderSlot;
  suffix?: RenderSlot;
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
  const Label = renderSlotWith(label, {
    variant: 'body',
    className: 'layout-label',
  });

  const Description = renderSlotWith(description, {
    variant: 'subheadline',
    className: 'layout-description',
  });

  const Suffix = renderSlotWith(suffix, {
    variant: 'subheadline',
    className: 'layout-suffix',
  });

  const Instruction = renderSlotWith(instruction, {
    variant: 'subheadline',
    className: 'layout-instruction',
  });

  const Action = renderSlotWith(action, {
    className: 'layout-action layout-action--flex-auto',
  });

  const ActionSlot = renderSlotWith(actionSlot, {
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
