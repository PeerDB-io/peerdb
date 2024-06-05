import styled, { css } from 'styled-components';

const variants = {
  button: css``,
  extended: css`
    width: 50%;
    min-width: ${({ theme }) => theme.size.xxSmall};
    max-width: ${({ theme }) => theme.size.medium};
  `,
  normal: css`
    min-width: ${({ theme }) => theme.size.xxSmall};
    max-width: ${({ theme }) => theme.size.xSmall};
  `,
  mirror_name: css`
    overflow: auto;
    min-width: ${({ theme }) => theme.size.small};
    max-width: ${({ theme }) => theme.size.medium};
  `,
};

export type TableCellVariant = keyof typeof variants;
type BaseTableCellProps = {
  $variant: TableCellVariant;
};
export const BaseTableCell = styled.td<BaseTableCellProps>`
  overflow: hidden;
  ${({ $variant }) => variants[$variant]}
`;
