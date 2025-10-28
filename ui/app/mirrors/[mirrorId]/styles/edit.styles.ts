import { CSSProperties } from 'react';

type Theme = 'light' | 'dark';

export const notPausedCalloutStyle = (theme: Theme): CSSProperties => ({
  marginTop: '1rem',
  backgroundColor: theme === 'dark' ? '#2d1a1a' : '#fef2f2',
  borderLeft: `4px solid ${theme === 'dark' ? '#f87171' : '#f87171'}`,
  padding: '1rem',
  borderRadius: '0.375rem',
  color: theme === 'dark' ? '#f87171' : '#991b1b',
});

export const tablesSelectedCalloutStyle = (theme: Theme): CSSProperties => ({
  marginTop: '1rem',
  backgroundColor: theme === 'dark' ? '#1f2937' : '#f3f4f6',
  borderLeft: `4px solid ${theme === 'dark' ? '#6b7280' : '#9ca3af'}`,
  padding: '1rem',
  borderRadius: '0.375rem',
  color: theme === 'dark' ? '#d1d5db' : '#374151',
});

export const tablesSelectedCalloutHeaderStyle: CSSProperties = {
  fontWeight: 'bold',
  marginBottom: '0.5rem',
};

export const fieldStyle: CSSProperties = {
  display: 'flex',
  flexDirection: 'row',
  alignItems: 'center',
};
