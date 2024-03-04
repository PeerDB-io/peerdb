import { CSSProperties } from 'styled-components';

export const tableStyle: CSSProperties = {
  maxHeight: '35vh',
  padding: '0.5rem',
  overflowY: 'auto',
  borderRadius: '0.5rem',
  border: '1px solid rgba(0,0,0,0.1)',
  marginTop: '0.5rem',
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
