import { CSSProperties } from 'react';
import type { DefaultTheme } from 'styled-components';

export const tableStyle = (theme: DefaultTheme): CSSProperties => ({
  padding: '0.5rem',
  borderRadius: '0.5rem',
  border: `1px solid ${theme.colors.base.border.subtle}`,
  marginTop: '0.5rem',
});

export const tableStyleMaxHeight = (theme: DefaultTheme): CSSProperties => ({
  ...tableStyle(theme),
  maxHeight: '35vh',
  overflowY: 'auto',
});
