'use client';

import { PropsWithChildren } from 'react';
import { ThemeProvider, createGlobalStyle } from 'styled-components';
import { appTheme, primitives } from './appTheme';

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
    
    * {
      box-sizing: border-box !important;
    }
  }
`;

export function AppThemeProvider({ children }: PropsWithChildren) {
  return (
    <ThemeProvider theme={appTheme}>
      <GlobalStyle />
      {children}
    </ThemeProvider>
  );
}
