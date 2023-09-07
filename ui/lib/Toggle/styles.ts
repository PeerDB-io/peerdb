import { css } from 'styled-components';

export const baseButtonStyle = css`
  all: unset;
  display: inline-flex;
  align-items: center;
  margin: ${({ theme }) => theme.spacing.xxSmall};
  border-radius: ${({ theme }) => theme.radius.small};
  padding: ${({ theme }) => `${theme.radius.xxSmall} ${theme.radius.small}`};
  color: ${({ theme }) => theme.colors.base.text.lowContrast};

  ${({ theme }) => theme.text.medium.body}

  &:hover:not([data-disabled]) {
    background-color: ${({ theme }) => theme.colors.base.surface.hovered};
    color: ${({ theme }) => theme.colors.base.text.highContrast};
  }

  &:focus {
    outline: 0;
  }

  &:focus-visible {
    outline: 2px solid ${({ theme }) => theme.colors.accent.border.normal};
    outline-offset: -2px;
  }

  &[data-state='on'],
  &[data-state='on']:hover:not([data-disabled]) {
    background-color: ${({ theme }) => theme.colors.base.surface.selected};
    color: ${({ theme }) => theme.colors.base.text.highContrast};
  }

  &[data-disabled] {
    background-color: transparent;
    opacity: 0.3;
  }
`;
