import styled, { css } from 'styled-components';

const variants = {
  default: css``,
  focused: css`
    outline: 2px solid ${({ theme }) => theme.colors.accent.border.normal};
    outline-width: -2px;
  `,
  disabled: css`
    opacity: 0.3;
  `,
};

export type RowVariant = keyof typeof variants;
type RowContainerProps = {
  $variant: RowVariant;
};

export const RowContainer = styled.div<RowContainerProps>`
  display: inline-block;
  margin: ${({ theme }) => theme.spacing.xxSmall} 0px;
  padding: ${({ theme }) => theme.spacing.medium};
  border-radius: ${({ theme }) => theme.spacing.medium};

  ${({ $variant }) => variants[$variant]}
`;

export const RowWrapper = styled.div`
  display: flex;
  flex-flow: row nowrap;
  align-items: flex-start;
  column-gap: ${({ theme }) => theme.spacing.medium};
`;

export const TextContent = styled.div`
  display: flex;
  grow: 1;
  flex-flow: column;
  row-gap: ${({ theme }) => theme.spacing.xxSmall};
`;

export const StyledPreTitle = styled.span`
  color: ${({ theme }) => theme.colors.base.text.lowContrast};
  ${({ theme }) => theme.text.regular.subheadline};
`;

export const StyledTitle = styled.span`
  flex: 1;

  color: ${({ theme }) => theme.colors.base.text.highContrast};
  ${({ theme }) => theme.text.medium.body};
`;

export const StyledTitleSuffix = styled.span`
  color: ${({ theme }) => theme.colors.base.text.lowContrast};
  ${({ theme }) => theme.text.regular.subheadline};
`;

export const StyledDescription = styled.span`
  flex: 1;

  color: ${({ theme }) => theme.colors.base.text.lowContrast};
  ${({ theme }) => theme.text.regular.subheadline};
`;

export const StyledDescriptionSuffix = styled.span`
  color: ${({ theme }) => theme.colors.base.text.lowContrast};
  ${({ theme }) => theme.text.regular.subheadline};
`;

export const StyledFootnote = styled.span`
  color: ${({ theme }) => theme.colors.base.text.lowContrast};
  ${({ theme }) => theme.text.regular.footnote};
`;
