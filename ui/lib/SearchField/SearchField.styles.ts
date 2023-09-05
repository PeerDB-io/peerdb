import styled, { css } from 'styled-components';

export const StyledWrapper = styled.div`
  --border-color: ${({ theme }) => theme.colors.base.border.normal};

  display: flex;
  align-items: flex-start;
  margin: ${({ theme }) => theme.spacing.xxSmall} 0px;
  padding: ${({ theme }) => `${theme.spacing.xxSmall} ${theme.spacing.medium}`};
  column-gap: ${({ theme }) => theme.spacing.medium};
  border-radius: ${({ theme }) => theme.radius.medium};
  border: 1px solid var(--border-color);

  color: ${({ theme }) => theme.colors.base.text.highContrast};
  ${({ theme }) => css(theme.text.regular.body)};

  &:focus-within {
    outline: 2px solid ${({ theme }) => theme.colors.accent.border.normal};
    outline-offset: -2px;
  }

  &:hover {
    --border-color: ${({ theme }) => theme.colors.base.border.hovered};
  }

  &[data-disabled] {
    opacity: 0.3;
  }
`;

export const BaseInput = styled.input`
  all: unset;

  flex: 1 1 auto;

  &::placeholder {
    color: ${({ theme }) => theme.colors.base.text.lowContrast};
  }

  &:focus {
    outline: none;
  }
`;
