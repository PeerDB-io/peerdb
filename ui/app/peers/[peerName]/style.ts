import { CSSProperties } from 'react';

export const tableStyle: CSSProperties = {
  padding: '0.5rem',
  borderRadius: '0.5rem',
  border: '1px solid rgba(0,0,0,0.1)',
  marginTop: '0.5rem',
};

export const tableStyleMaxHeight: CSSProperties = {
  ...tableStyle,
  maxHeight: '35vh',
  overflowY: 'auto',
};

export const connStringStyle: CSSProperties = {
  backgroundColor: 'white',
  display: 'flex',
  width: 'fit-content',
  alignItems: 'center',
  padding: '0.5rem',
  border: '1px solid rgba(0,0,0,0.1)',
  borderRadius: '0.5rem',
  marginTop: '0.5rem',
  fontFamily: 'monospace',
  whiteSpace: 'pre-wrap',
};
