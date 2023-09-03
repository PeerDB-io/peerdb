import { css, styled } from 'styled-components';

export const baseStyles = css`
  --border-color: ${({ theme }) => theme.colors.base.border.normal};

  display: flex;
  width: 100%;
  padding: ${({ theme }) => `${theme.spacing.xxSmall} ${theme.spacing.medium}`};
  margin: ${({ theme }) => `${theme.spacing.xxSmall} 0px`};
  border-radius: ${({ theme }) => theme.radius.xSmall};
  border: 1px solid var(--border-color);
  background-color: ${({ theme }) => theme.colors.base.background.normal};

  color: ${({ theme }) => theme.colors.base.text.highContrast};
  ${({ theme }) => theme.text.regular.body};

  &::placeholder {
    color: ${({ theme }) => theme.colors.base.text.lowContrast};
  }

  &:focus {
    outline: none;
  }

  &:hover {
    --border-color: ${({ theme }) => theme.colors.base.border.hovered};
  }

  &:focus-visible {
    outline: 2px solid ${({ theme }) => theme.colors.accent.border.normal};
    outline-offset: -2px;
  }

  &:disabled {
    opacity: 0.3;
  }
`;

export const BaseInputField = styled.input`
  height: 28px;
  ${baseStyles}
`;

export const BaseTextArea = styled.textarea`
  resize: none;
  min-height: ${({ theme }) => theme.size.xxSmall};
  ${baseStyles}
`;
