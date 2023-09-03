import { Decorator } from '@storybook/react';
import { AppThemeProvider } from '../lib/AppTheme';

export const withTheme: Decorator = (Story) => (
  <AppThemeProvider>
    <Story />
  </AppThemeProvider>
);
