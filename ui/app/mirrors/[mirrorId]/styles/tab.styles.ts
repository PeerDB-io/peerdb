import { CSSProperties } from 'react';
type Theme = 'light' | 'dark';
export const TabsRootStyle: CSSProperties = {
  display: 'flex',
  flexDirection: 'column',
  marginTop: '1rem',
};
export const TabListStyle = (theme: Theme): CSSProperties => ({
  display: 'flex',
  flexDirection: 'row',
  justifyContent: 'space-around',
  borderBottom:
    theme === 'dark'
      ? '1px solid rgba(255,255,255,0.1)'
      : '1px solid rgba(0,0,0,0.1)',
  paddingBottom: '1rem',
});

export const notPausedCalloutStyle = (theme: Theme): CSSProperties => ({
  marginTop: '1rem',
  backgroundColor: theme === 'dark' ? '#2d1a1a' : '#fef2f2',
  borderLeft: `4px solid ${theme === 'dark' ? '#f87171' : '#f87171'}`,
  padding: '1rem',
  borderRadius: '0.375rem',
  color: theme === 'dark' ? '#f87171' : '#991b1b',
});
