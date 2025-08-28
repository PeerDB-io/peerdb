'use client';

import React, { createContext, useContext, useEffect, useState, useMemo } from 'react';
import { ThemeProvider as StyledThemeProvider } from 'styled-components';
import { appTheme } from './appTheme';
import { appThemeColors } from './appThemeColors';
import { appThemeColorsDark } from './appThemeColorsDark';


export type ThemeMode = 'light' | 'dark';

interface ThemeContextType {
  theme: ThemeMode;
  toggleTheme: () => void;
  setTheme: (theme: ThemeMode) => void;
}

const ThemeContext = createContext<ThemeContextType | undefined>(undefined);

const THEME_COOKIE_NAME = 'peerdb-theme';

export function ThemeProvider({ children, initialTheme = 'light' }: { children: React.ReactNode; initialTheme?: ThemeMode }) {
  // Use the theme from SSR (cookie-based)
  const [theme, setThemeState] = useState<ThemeMode>(initialTheme);
  
  useEffect(() => {
    // Update DOM class when theme changes
    const root = document.documentElement;
    root.classList.remove('light', 'dark');
    root.classList.add(theme);
    
    // Store in cookie for SSR
    document.cookie = `${THEME_COOKIE_NAME}=${theme}; path=/; max-age=${60 * 60 * 24 * 365}; SameSite=Lax`;
  }, [theme]);

  const toggleTheme = () => {
    setThemeState((prev) => (prev === 'light' ? 'dark' : 'light'));
  };

  const setTheme = (newTheme: ThemeMode) => {
    setThemeState(newTheme);
  };

  const styledTheme = useMemo(() => {
    const colors = theme === 'dark' ? appThemeColorsDark : appThemeColors;
    return {
      ...appTheme,
      colors: colors as typeof appThemeColors,
    };
  }, [theme]);

  return (
    <ThemeContext.Provider value={{ theme, toggleTheme, setTheme }}>
      <StyledThemeProvider theme={styledTheme}>
        {children}
      </StyledThemeProvider>
    </ThemeContext.Provider>
  );
}

export function useTheme() {
  const context = useContext(ThemeContext);
  if (context === undefined) {
    throw new Error('useTheme must be used within ThemeProvider');
  }
  return context;
}