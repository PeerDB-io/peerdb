import * as RadixDialog from '@radix-ui/react-dialog';
import styled, { css } from 'styled-components';

const sizes = {
  xxSmall: css`
    width: ${({ theme }) => theme.size.xSmall};
    ${css(({ theme }) => theme.dropShadow.large)};
  `,
  xSmall: css`
    width: ${({ theme }) => theme.size.small};
    ${css(({ theme }) => theme.dropShadow.large)};
  `,
  small: css`
    width: ${({ theme }) => theme.size.medium};
    ${css(({ theme }) => theme.dropShadow.xLarge)};
  `,
  medium: css`
    width: ${({ theme }) => theme.size.large};
    ${css(({ theme }) => theme.dropShadow.xLarge)};
  `,
  large: css`
    width: ${({ theme }) => theme.size.xLarge};
    ${css(({ theme }) => theme.dropShadow.xxLarge)};
  `,
  xLarge: css`
    width: ${({ theme }) => theme.size.xxLarge};
    ${css(({ theme }) => theme.dropShadow.xxLarge)};
  `,
};

export type DialogSize = keyof typeof sizes;
type StyledWrapperProps = {
  $size: DialogSize;
};
export const BaseContent = styled(RadixDialog.Content)<StyledWrapperProps>`
  display: flex;
  padding: ${({ theme }) => theme.spacing.medium};

  position: fixed;
  top: 50%;
  left: 50%;
  translate: -50%;

  border: 1px solid ${({ theme }) => theme.colors.base.border.subtle};
  border-radius: ${({ theme }) => theme.radius.medium};
  background-color: ${({ theme }) => theme.colors.base.background.normal};

  ${({ $size }) => sizes[$size]}

  &:focus {
    outline: 2px solid ${({ theme }) => theme.colors.accent.border.normal};
    outline-offset: -2px;
  }
`;
