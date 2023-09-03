import { PropsWithChildren } from 'react';
import { PolymorphicComponentProps, RenderSlot } from '../types';
import { isDefined } from '../utils/isDefined';
import { renderSlotWith } from '../utils/renderSlotWith';
import { BaseItem, StyledLabel, StyledSuffix } from './SidebarItem.styles';

type SidebarItemProps = PropsWithChildren<{
  selected?: boolean;
  disabled?: boolean;
  leadingIcon?: RenderSlot;
  suffix?: string;
  trailingIcon?: RenderSlot;
}>;

export function SidebarItem<AsTarget extends React.ElementType>({
  children,
  leadingIcon,
  suffix,
  trailingIcon,
  selected,
  disabled,
  ...baseItemProps
}: PolymorphicComponentProps<AsTarget, SidebarItemProps>) {
  const LeadingIcon = renderSlotWith(leadingIcon, {
    className: 'sidebar-item-icon--leading',
  });

  const TrailingIcon = renderSlotWith(trailingIcon, {
    className: 'sidebar-item-icon--trailing',
  });

  const Suffix = isDefined(suffix) && (
    <StyledSuffix variant='body'>{suffix}</StyledSuffix>
  );

  return (
    <BaseItem
      {...baseItemProps}
      data-selected={selected || undefined}
      data-disabled={disabled || undefined}
    >
      {LeadingIcon}
      <StyledLabel variant='body'>{children}</StyledLabel>
      {Suffix}
      {TrailingIcon}
    </BaseItem>
  );
}
