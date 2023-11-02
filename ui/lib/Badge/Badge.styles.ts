import styled, { css } from 'styled-components';

const normalStyle = css`
  --background-color: ${({ theme }) => theme.colors.base.surface.normal};
  --text-color: ${({ theme }) => theme.colors.base.text.highContrast};
`;

const accentStyle = css`
  --background-color: ${({ theme }) => theme.colors.accent.surface.normal};
  --text-color: ${({ theme }) => theme.colors.accent.text.highContrast};
`;

const positiveStyle = css`
  --background-color: ${({ theme }) => theme.colors.positive.surface.normal};
  --text-color: ${({ theme }) => theme.colors.positive.text.highContrast};
`;

const warningStyle = css`
  --background-color: ${({ theme }) => theme.colors.warning.surface.normal};
  --text-color: ${({ theme }) => theme.colors.warning.text.highContrast};
`;

const destructiveStyle = css`
  --background-color: ${({ theme }) => theme.colors.destructive.surface.normal};
  --text-color: ${({ theme }) => theme.colors.destructive.text.highContrast};
`;

const variants = {
  normal: normalStyle,
  accent: accentStyle,
  positive: positiveStyle,
  warning: warningStyle,
  destructive: destructiveStyle,
};

const defaultTypeStyle = css``;
const singleDigitTypeStyle = css`
  padding: 0;
  width: 24px;
  height: 24px;
  justify-content: center;
`;
const longTextTypeStyle = css`
  padding: 10;
  width: max-content;
  height: 32px;
  justify-content: center;
`;
const types = {
  default: defaultTypeStyle,
  singleDigit: singleDigitTypeStyle,
  longText: longTextTypeStyle,
};

const baseStyle = css`
  display: inline-flex;
  flex-direction: row;
  column-gap: ${({ theme }) => theme.spacing.xSmall};
  width: min-content;
  margin: ${({ theme }) => theme.spacing.xxSmall} 0px;
  padding: ${({ theme }) => `${theme.spacing.xxSmall} ${theme.spacing.medium}`};

  border-radius: ${({ theme }) => theme.radius['3xLarge']};

  background-color: var(--background-color);
  color: var(--text-color);
  ${({ theme }) => css(theme.text.regular.subheadline)}
`;

export type BadgeVariant = keyof typeof variants;
export type BadgeType = keyof typeof types;
type BadgeWrapperProps = {
  $variant: BadgeVariant;
  $type: BadgeType;
};
export const BadgeWrapper = styled.div<BadgeWrapperProps>`
  ${baseStyle}
  ${({ $variant }) => variants[$variant]}
  ${({ $type }) => types[$type]}
`;
