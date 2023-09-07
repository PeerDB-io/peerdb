import * as RadixSelect from '@radix-ui/react-select';
import styled, { css } from 'styled-components';

export const StyledItem = styled(RadixSelect.Item)`
  position: relative;
  display: flex;
  justify-content: flex-start;

  padding: 0;
  padding-left: ${({ theme }) => theme.spacing['4xLarge']};
  padding-right: ${({ theme }) => theme.spacing.medium};

  border-radius: ${({ theme }) => theme.radius.small};

  color: ${({ theme }) => theme.colors.base.text.highContrast};
  ${({ theme }) => css(theme.text.regular.body)}

  &[data-state="checked"],
  &[data-highlighted] {
    background-color: ${({ theme }) => theme.colors.base.surface.hovered};
    outline: none;
  }

  &[data-disabled] {
    opacity: 0.3;
  }
`;

export const StyledIndicator = styled(RadixSelect.ItemIndicator)`
  position: absolute;
  left: ${({ theme }) => theme.spacing.medium};
`;
