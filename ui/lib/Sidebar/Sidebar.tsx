import { PropsWithChildren } from 'react';
import { Separator } from '../Separator';
import { PolymorphicComponentProps, RenderSlot } from '../types';
import { isDefined } from '../utils/isDefined';
import {
  BottomRowWrapper,
  StyledItemWrapper,
  StyledWrapper,
} from './Sidebar.styles';

type SidebarProps = PropsWithChildren<{
  topTitle?: RenderSlot;
  avatar?: RenderSlot;
  selectButton?: RenderSlot;
  bottomRow?: RenderSlot;
  bottomLabel?: RenderSlot;
  className?: string;
}>;

export function Sidebar({
  avatar,
  bottomRow,
  bottomLabel,
  selectButton,
  children,
  topTitle,
  ...wrapperProps
}: PolymorphicComponentProps<'div', SidebarProps>) {
  const TopTitle = isDefined(topTitle) && (
    <>
      {topTitle()}
      <Separator variant='empty' height='tall' />
    </>
  );

  const Avatar = isDefined(avatar) && avatar();

  const BottomRow = isDefined(bottomRow) && (
    <>
      <BottomRowWrapper>{bottomRow()}</BottomRowWrapper>
      <Separator variant='empty' height='thin' />
    </>
  );

  const BottomLabel = isDefined(bottomLabel) && bottomLabel();

  const SelectButton = isDefined(selectButton) && (
    <>
      {selectButton()}
      <Separator variant='empty' height='tall' />
    </>
  );

  return (
    <StyledWrapper {...wrapperProps}>
      {TopTitle}
      {Avatar}
      {SelectButton}
      <StyledItemWrapper>{children}</StyledItemWrapper>
      {BottomRow}
      {BottomLabel}
    </StyledWrapper>
  );
}
