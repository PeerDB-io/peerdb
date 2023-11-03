import styled, { css } from 'styled-components';

type BaseStyleProps = {
  $loading: boolean;
};

export const baseStyle = css<BaseStyleProps>`
  display: flex;
  column-gap: ${({ theme }) => theme.spacing.xSmall};
  padding: ${({ theme }) => `${theme.spacing.xxSmall} ${theme.spacing.medium}`};
  margin: ${({ theme }) => `${theme.spacing.xxSmall} 0px`};
  justify-content: center;
  align-items: center;

  border-radius: ${({ theme }) => theme.radius.medium};
  appearance: none;
  border: 0;
  background: 0;

  cursor: pointer;

  ${({ theme }) => css(theme.text.medium.body)}
  color: var(--text-color);
  background-color: var(--background-color-default);

  &:hover:not(:disabled) {
    background-color: var(--background-color-hover);
  }

  &:focus {
    outline: none;
  }

  &:focus-visible:not(:disabled) {
    outline: 2px solid var(--focus-border-color);
    outline-offset: -2px;
    background-color: var(--background-color-focus);
  }

  &:disabled {
    opacity: 0.3;
    cursor: not-allowed;
  }
`;

const normalStyle = css<BaseStyleProps>`
  --focus-border-color: ${({ theme }) => theme.colors.accent.border.normal};
  --text-color: ${({ theme }) => theme.colors.base.text.highContrast};
  --background-color-default: ${({ theme, $loading }) =>
    $loading
      ? theme.colors.base.surface.selected
      : theme.colors.base.surface.normal};
  --background-color-hover: ${({ theme }) => theme.colors.base.surface.hovered};
  --background-color-focus: ${({ theme }) => theme.colors.base.surface.normal};
`;

const dropStyle = css<BaseStyleProps>`
  --focus-border-color: ${({ theme }) => theme.colors.accent.border.normal};
  --text-color: ${({ theme }) => theme.colors.base.text.highContrast};
  --background-color-default: ${({ theme, $loading }) =>
    $loading ? theme.colors.base.surface.selected : 'transparent'};
  --background-color-hover: ${({ theme }) =>
    theme.colors.destructive.surface.hovered};
  --background-color-focus: transparent;
`;

const destructiveStyle = css<BaseStyleProps>`
  --focus-border-color: ${({ theme }) => theme.colors.accent.border.normal};
  --text-color: ${({ theme }) => theme.colors.destructive.text.lowContrast};
  --background-color-default: ${({ theme, $loading }) =>
    $loading
      ? theme.colors.destructive.surface.selected
      : theme.colors.destructive.surface.normal};
  --background-color-hover: ${({ theme }) =>
    theme.colors.destructive.surface.hovered};
  --background-color-focus: ${({ theme }) =>
    theme.colors.destructive.surface.normal};
`;

const normalSolidStyle = css<BaseStyleProps>`
  --focus-border-color: ${({ theme }) => theme.colors.accent.border.normal};
  --text-color: ${({ theme }) => theme.colors.special.fixed.white};
  --background-color-default: ${({ theme, $loading }) =>
    $loading
      ? theme.colors.accent.fill.hovered
      : theme.colors.accent.fill.normal};
  --background-color-hover: ${({ theme }) => theme.colors.accent.fill.hovered};
  --background-color-focus: ${({ theme }) => theme.colors.accent.fill.normal};
`;

const destructiveSolidStyle = css<BaseStyleProps>`
  --focus-border-color: ${({ theme }) => theme.colors.accent.border.normal};
  --text-color: ${({ theme }) => theme.colors.special.fixed.white};
  --background-color-default: ${({ theme, $loading }) =>
    $loading
      ? theme.colors.destructive.fill.hovered
      : theme.colors.destructive.fill.normal};
  --background-color-hover: ${({ theme }) =>
    theme.colors.destructive.fill.hovered};
  --background-color-focus: ${({ theme }) =>
    theme.colors.destructive.fill.normal};
`;

const normalBorderlessStyle = css<BaseStyleProps>`
  --focus-border-color: ${({ theme }) => theme.colors.accent.border.normal};
  --text-color: ${({ theme }) => theme.colors.base.text.highContrast};
  --background-color-default: ${({ theme, $loading }) =>
    $loading ? theme.colors.base.surface.selected : 'transparent'};
  --background-color-hover: ${({ theme }) => theme.colors.base.surface.hovered};
  --background-color-focus: transparent;
`;

const destructiveBorderlessStyle = css<BaseStyleProps>`
  --focus-border-color: ${({ theme }) => theme.colors.accent.border.normal};
  --text-color: ${({ theme }) => theme.colors.destructive.text.lowContrast};
  --background-color-default: ${({ theme, $loading }) =>
    $loading ? theme.colors.destructive.surface.selected : 'transparent'};
  --background-color-hover: ${({ theme }) =>
    theme.colors.destructive.surface.hovered};
  --background-color-focus: transparent;
`;

const variants = {
  normal: normalStyle,
  destructive: destructiveStyle,
  normalSolid: normalSolidStyle,
  destructiveSolid: destructiveSolidStyle,
  drop: dropStyle,
  normalBorderless: normalBorderlessStyle,
  destructiveBorderless: destructiveBorderlessStyle,
} as const;

export type ButtonVariant = keyof typeof variants;
export type BaseButtonProps = {
  $variant: ButtonVariant;
} & BaseStyleProps;

export const BaseButton = styled.button<BaseButtonProps>`
  ${baseStyle}
  ${({ $variant }) => variants[$variant]}
`;
