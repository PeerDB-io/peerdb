import { css, styled } from 'styled-components';

const heights = {
  thin: css`
    --height: ${({ theme }) => theme.spacing.medium};
  `,
  tall: css`
    --height: ${({ theme }) => theme.spacing.xLarge};
  `,
};

const variants = {
  normal: css`
    --line-height: 1px;
    --line-width: 100%;
  `,
  thick: css`
    --line-height: 2px;
    --line-width: 100%;
  `,
  indent: css`
    --line-height: 1px;
    --line-width: calc(100% - ${({ theme }) => theme.spacing.medium});
    &::before {
      left: ${({ theme }) => theme.spacing.medium};
    }
  `,
  centered: css`
    --line-height: 1px;
    --line-width: calc(100% - ${({ theme }) => theme.spacing.medium} * 2);
    &::before {
      left: ${({ theme }) => theme.spacing.medium};
    }
  `,
  empty: css`
    &::before {
      content: none;
    }
  `,
};

export type SeparatorHeight = keyof typeof heights;
export type SeparatorVariant = keyof typeof variants;

type BaseSeparatorProps = {
  $height: SeparatorHeight;
  $variant: SeparatorVariant;
};

export const BaseSeparator = styled.div<BaseSeparatorProps>`
  width: 100%;
  height: var(--height);
  position: relative;

  &::before {
    content: '';
    display: block;
    position: absolute;
    top: 50%;
    translate: 0 -50%;
    left: 0;
    right: 0;
    width: var(--line-width);
    height: var(--line-height);
    background-color: ${({ theme }) => theme.colors.base.border.subtle};
  }

  ${({ $height }) => heights[$height]}
  ${({ $variant }) => variants[$variant]}
`;
