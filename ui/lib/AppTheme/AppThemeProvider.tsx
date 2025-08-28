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
`;

export function AppThemeProvider({ children, initialTheme = 'light' }: PropsWithChildren<{ initialTheme?: 'light' | 'dark' }>) {
  return (
    <ThemeProvider initialTheme={initialTheme}>
      <GlobalStyle />
      {children}
    </ThemeProvider>
  );
}
