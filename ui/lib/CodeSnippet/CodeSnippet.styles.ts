import styled from 'styled-components';
import { baseStyles } from '../TextField/TextField.styles';

export const BaseTextArea = styled.textarea`
  ${baseStyles}
  resize: none;
  min-height: ${({ theme }) => theme.size.large};
  ${({ theme }) => theme.text.mono.snippet}
  color: ${({ theme }) => theme.colors.base.text.highContrast};

  &:focus-visible {
    color: ${({ theme }) => theme.colors.base.text.lowContrast};
  }

  -ms-overflow-style: none;
  scrollbar-width: none;

  &::-webkit-scrollbar {
    display: none;
  }
`;
