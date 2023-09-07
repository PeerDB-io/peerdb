'use client';
import { PropsWithChildren } from 'react';
import { RenderObject } from '../types';
import { isDefined } from '../utils/isDefined';
import { BaseLayoutProps } from './BaseLayout';
import { BaseLayoutColumn, BaseLayoutColumnProps } from './BaseLayoutColumn';
import { BaseLayoutRow } from './BaseLayoutRow';
import {
  ContentWrapper,
  LayoutRightSidebarWrapper,
  LayoutWrapper,
  StyledMain,
  StyledMainProps,
} from './Layout.styles';

type RowWithButtonProps = BaseLayoutProps;
export function RowWithButton({ ...baseRowProps }: RowWithButtonProps) {
  return <BaseLayoutRow {...baseRowProps} actionPosition='right' />;
}

type RowWithRadioButtonProps = BaseLayoutProps;
export function RowWithRadiobutton({
  ...baseRowProps
}: RowWithRadioButtonProps) {
  return <BaseLayoutRow {...baseRowProps} actionPosition='left' />;
}

type RowWithCheckboxProps = BaseLayoutProps;
export function RowWithCheckbox({ ...baseRowProps }: RowWithCheckboxProps) {
  return <BaseLayoutRow {...baseRowProps} actionPosition='left' />;
}

type RowWithSwitchProps = BaseLayoutProps;
export function RowWithSwitch({ ...baseRowProps }: RowWithSwitchProps) {
  return <BaseLayoutRow {...baseRowProps} actionPosition='right' />;
}

type RowWithTextFieldProps = BaseLayoutProps;
export function RowWithTextField({ ...baseRowProps }: RowWithTextFieldProps) {
  return <BaseLayoutRow {...baseRowProps} actionPosition='right' actionFlex />;
}

type RowWithProgressBarProps = BaseLayoutColumnProps;
export function RowWithProgressBar({
  ...baseColumnProps
}: RowWithProgressBarProps) {
  return <BaseLayoutColumn {...baseColumnProps} />;
}

type ColumnWithTextFieldProps = BaseLayoutColumnProps;
export function ColumnWithTextField({
  ...baseColumnProps
}: ColumnWithTextFieldProps) {
  return <BaseLayoutColumn {...baseColumnProps} />;
}

type RowWithSelectProps = BaseLayoutProps;
export function RowWithSelect({ ...baseRowProps }: RowWithSelectProps) {
  return <BaseLayoutRow {...baseRowProps} actionPosition='right' actionFlex />;
}

type RowWithToggleGroupProps = Pick<
  BaseLayoutProps,
  'label' | 'action' | 'className'
>;
export function RowWithToggleGroup({
  ...baseRowProps
}: RowWithToggleGroupProps) {
  return (
    <BaseLayoutRow
      {...baseRowProps}
      actionPosition='right'
      actionFlex
      actionFlexEnd
    />
  );
}

type LayoutProps = PropsWithChildren<{
  sidebar?: RenderObject;
}>;
export function Layout({ sidebar, children }: LayoutProps) {
  const Sidebar = isDefined(sidebar) && sidebar;
  const fullWidth = !isDefined(sidebar);

  return (
    <LayoutWrapper>
      {Sidebar}
      <ContentWrapper $fullWidth={fullWidth}>{children}</ContentWrapper>
    </LayoutWrapper>
  );
}

type LayoutMainProps = PropsWithChildren<{
  alignSelf: StyledMainProps['$alignSelf'];
  justifySelf: StyledMainProps['$justifySelf'];
  width?: StyledMainProps['$width'];
  topPadding?: StyledMainProps['$topPadding'];
}>;
export function LayoutMain({
  children,
  alignSelf,
  justifySelf,
  width = 'full',
  topPadding = false,
}: LayoutMainProps) {
  return (
    <StyledMain
      $alignSelf={alignSelf}
      $justifySelf={justifySelf}
      $width={width}
      $topPadding={topPadding}
    >
      {children}
    </StyledMain>
  );
}
LayoutMain.displayName = 'LayoutMain';

type LayoutRightSidebarProps = PropsWithChildren<{
  open?: boolean;
  className?: string;
}>;

export function LayoutRightSidebar({
  open = false,
  children,
  ...divProps
}: LayoutRightSidebarProps) {
  return (
    <LayoutRightSidebarWrapper data-open={open} {...divProps}>
      {children}
    </LayoutRightSidebarWrapper>
  );
}
