'use client';
import { PropsWithChildren } from 'react';
import { LabelProps } from '../Label';
import { PolymorphicComponentProps, RenderObject } from '../types';
import { isDefined } from '../utils/isDefined';
import { HeaderWrapper, StyledLabel } from './Header.styles';

type HeaderProps = {
  slot?: RenderObject;
} & LabelProps &
  PropsWithChildren;

export function Header<AsTarget extends React.ElementType>({
  slot,
  children,
  className,
  ...labelProps
}: PolymorphicComponentProps<AsTarget, HeaderProps>) {
  const Slot = isDefined(slot) && slot;

  return (
    <HeaderWrapper className={className}>
      <StyledLabel {...(labelProps as LabelProps)}>{children}</StyledLabel>
      {Slot}
    </HeaderWrapper>
  );
}
