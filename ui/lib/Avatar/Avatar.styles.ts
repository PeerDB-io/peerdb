import styled, { css } from 'styled-components';
import { Icon } from '../Icon';

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

export type AvatarSize = keyof typeof sizes;

type AvatarBaseProps = {
  $size: AvatarSize;
};

const avatarBaseStyle = css<AvatarBaseProps>`
  display: flex;
  justify-content: center;
  align-items: center;
  border-radius: ${({ theme }) => theme.radius['3xLarge']};
  background-color: ${({ theme }) => theme.colors.base.fill.normal};
  ${({ $size }) => sizes[$size]}
`;

export const AvatarImageBase = styled.img<AvatarBaseProps>`
  ${avatarBaseStyle}

  object-fit: cover;
  object-position: center;
`;

export const AvatarTextBase = styled.span<AvatarBaseProps>`
  ${avatarBaseStyle}

  color: ${({ theme }) => theme.colors.special.fixed.white};
  ${({ theme, $size }) => {
    switch ($size) {
      case 'small':
      case 'medium':
        return css(theme.text.regular.caption1);
      case 'large':
        return css(theme.text.regular.footnote);
      case 'xLarge':
        return css(theme.text.regular.headline);
    }
  }}
`;

export const AvatarIconBase = styled(Icon)<AvatarBaseProps>`
  ${avatarBaseStyle}

  color: ${({ theme }) => theme.colors.special.fixed.white};
  ${({ theme, $size }) => {
    switch ($size) {
      case 'small':
      case 'medium':
        return css({
          fontSize: '16px',
          lineHeight: 'normal',
        });
      case 'large':
        return css({
          fontSize: '24px',
          lineHeight: 'normal',
        });
      case 'xLarge':
        return css({
          fontSize: '32px',
          lineHeight: 'normal',
        });
    }
  }}
`;
