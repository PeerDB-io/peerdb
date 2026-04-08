'use client';

import { PropsWithChildren } from 'react';
import { createGlobalStyle } from 'styled-components';
import { primitives } from './appTheme';
import { ThemeProvider } from './ThemeContext';

import './tailwind.css';

const GlobalStyle = createGlobalStyle`
  html {
    font-family: ${primitives.typography.textFont};
    font-size: 100%;
    -webkit-font-smoothing: antialiased;
    font-smooth: always;
  }
  html,
  body {
    padding: 0;
    margin: 0;
    background-color: ${({ theme }) => theme.colors.base.background.normal};
    color: ${({ theme }) => theme.colors.base.text.highContrast};
    transition: background-color 0.2s ease, color 0.2s ease;

    * {
      box-sizing: border-box !important;
    }
  }

  /* Custom scrollbar styling for dark mode support */
  * {
    scrollbar-width: thin;
    scrollbar-color: ${({ theme }) => theme.colors.base.border.normal} ${({ theme }) => theme.colors.base.background.normal};
  }

  *::-webkit-scrollbar {
    width: 8px;
    height: 8px;
  }

  *::-webkit-scrollbar-track {
    background: ${({ theme }) => theme.colors.base.background.normal};
  }

  *::-webkit-scrollbar-thumb {
    background-color: ${({ theme }) => theme.colors.base.border.normal};
    border-radius: 4px;
    border: 2px solid ${({ theme }) => theme.colors.base.background.normal};
  }

  *::-webkit-scrollbar-thumb:hover {
    background-color: ${({ theme }) => theme.colors.base.border.hovered};
  }
`;

export function AppThemeProvider({
  children,
  initialTheme = 'light',
}: PropsWithChildren<{ initialTheme?: 'light' | 'dark' }>) {
  return (
    <ThemeProvider initialTheme={initialTheme}>
      <GlobalStyle />
      {children}
    </ThemeProvider>
  );
}
