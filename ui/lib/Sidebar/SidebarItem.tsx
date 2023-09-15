'use client';
import { PropsWithChildren } from 'react';
import { PolymorphicComponentProps, RenderObject } from '../types';
import { isDefined } from '../utils/isDefined';
import { renderObjectWith } from '../utils/renderObjectWith';
import { BaseItem, StyledLabel, StyledSuffix } from './SidebarItem.styles';

type SidebarItemProps = PropsWithChildren<{
  selected?: boolean;
  disabled?: boolean;
  leadingIcon?: RenderObject;
  suffix?: string;
  trailingIcon?: RenderObject;
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
  const LeadingIcon = renderObjectWith(leadingIcon, {
    className: 'sidebar-item-icon--leading',
  });

  const TrailingIcon = renderObjectWith(trailingIcon, {
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
