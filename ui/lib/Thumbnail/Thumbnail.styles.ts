import styled, { css } from 'styled-components';

const sizes = {
  small: css`
    width: ${({ theme }) => theme.spacing.xxLarge};
    height: ${({ theme }) => theme.spacing.xxLarge};
  `,
  medium: css`
    width: ${({ theme }) => theme.spacing['3xLarge']};
    height: ${({ theme }) => theme.spacing['3xLarge']};
  `,
  large: css`
    width: ${({ theme }) => theme.spacing['5xLarge']};
    height: ${({ theme }) => theme.spacing['5xLarge']};
  `,
  xLarge: css`
    width: ${({ theme }) => theme.spacing['6xLarge']};
    height: ${({ theme }) => theme.spacing['6xLarge']};
  `,
};

export type ThumbnailSize = keyof typeof sizes;

type BaseThumbnailProps = {
  $size: ThumbnailSize;
};

export const BaseThumbnail = styled.img<BaseThumbnailProps>`
  display: flex;
  justify-content: center;
  align-items: center;
  border: 0;
  border-radius: ${({ theme }) => theme.radius.xSmall};
  background-color: ${({ theme }) => theme.colors.base.background.normal};

  object-fit: cover;
  object-position: center;

  ${({ $size }) => sizes[$size]}
`;
