import styled from 'styled-components';

export const BaseAction = styled.a`
  display: inline-flex;
  align-items: center;
  flex-flow: row nowrap;
  column-gap: ${({ theme }) => theme.spacing.medium};

  padding: ${({ theme }) => `${theme.spacing.xSmall} ${theme.spacing.medium}`};
  text-decoration: none;

  color: ${({ theme }) => theme.colors.accent.text.lowContrast};
  ${({ theme }) => theme.text.medium.body}

  &:focus-visible:not([data-disabled]),
  &:hover:not([data-disabled]) {
    color: ${({ theme }) => theme.colors.accent.text.highContrast};
  }

  &:focus {
    outline: none;
  }

  &[data-disabled] {
    opacity: 0.3;
    pointer-events: none;
    user-select: none;
  }
`;
