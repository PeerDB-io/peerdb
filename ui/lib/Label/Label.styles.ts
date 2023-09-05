import styled, { css } from 'styled-components';
import { appThemeColors } from '../AppTheme/appThemeColors';

const baseStyle = css`
  all: unset;
  display: inline-block;
  padding: ${({ theme }) => `${theme.spacing.xSmall} ${theme.spacing.medium}`};
`;

const variants = {
  body: css`
    ${({ theme }) => theme.text.regular.body}
  `,
  action: css`
    text-decoration: none;
    ${({ theme }) => theme.text.medium.body}
  `,
  footnote: css`
    ${({ theme }) => theme.text.regular.footnote}
  `,
  subheadline: css`
    ${({ theme }) => theme.text.medium.subheadline}
  `,
  headline: css`
    ${({ theme }) => theme.text.semiBold.headline}
  `,
  title3: css`
    ${({ theme }) => theme.text.semiBold.title3}
  `,
  title2: css`
    ${({ theme }) => theme.text.semiBold.title2}
  `,
  title1: css`
    ${({ theme }) => theme.text.semiBold.title1}
  `,
  largeTitle: css`
    ${({ theme }) => theme.text.semiBold.largeTitle}
  `,
};

export type LabelVariant = keyof typeof variants;
export type LabelColorSet = keyof typeof appThemeColors;
export type LabelColorName = 'lowContrast' | 'highContrast';

type BaseLabelProps = {
  $variant: LabelVariant;
  $colorSet: LabelColorSet;
  $colorName: LabelColorName;
};
export const BaseLabel = styled.span<BaseLabelProps>`
  ${baseStyle}
  ${({ $variant }) => variants[$variant]}
  color: var(--text-color, ${({ theme, $colorSet, $colorName }) =>
    (theme.colors[$colorSet] as any).text[$colorName]});
`;
