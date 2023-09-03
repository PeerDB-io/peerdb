import * as RadixSelect from '@radix-ui/react-select';
import { css, styled } from 'styled-components';

export const StyledSelectIcon = styled(RadixSelect.Icon)`
  color: ${({ theme }) => theme.colors.base.text.lowContrast};
`;

export const StyledSelectTrigger = styled(RadixSelect.Trigger)`
  all: unset;

  min-width: 128px;
  width: 100%;
  margin: ${({ theme }) => theme.spacing.xxSmall} 0px;
  padding: ${({ theme }) => `${theme.spacing.xxSmall} ${theme.spacing.medium}`};

  display: flex;
  flex-flow: row nowrap;

  border-radius: ${({ theme }) => theme.radius.medium};
  color: ${({ theme }) => theme.colors.base.text.highContrast};
  ${({ theme }) => css(theme.text.regular.body)}

  &[data-state='open'],
  &:focus-visible,
  &:hover:not([data-disabled]) {
    justify-content: space-between;

    > i {
      color: ${({ theme }) => theme.colors.base.text.highContrast};
    }
  }

  &[data-state='open'],
  &:hover:not([data-disabled]) {
    background-color: ${({ theme }) => theme.colors.base.surface.hovered};
  }

  &:focus {
    outline: none;
  }

  &:focus-visible {
    outline: 2px solid ${({ theme }) => theme.colors.accent.border.normal};
    outline-offset: -2px;
  }

  &[data-disabled] {
    opacity: 0.3;
  }
`;
