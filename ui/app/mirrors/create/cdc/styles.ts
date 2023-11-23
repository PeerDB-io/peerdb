import { CSSProperties } from 'styled-components';

export const expandableStyle = {
  fontSize: 14,
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'space-between',
  color: 'rgba(0,0,0,0.7)',
  cursor: 'pointer',
};

export const schemaBoxStyle: CSSProperties = {
  width: '100%',
  marginTop: '0.5rem',
  padding: '0.5rem',
  display: 'flex',
  flexDirection: 'column',
  border: '1px solid #e9ecf2',
  borderRadius: '0.8rem',
};

export const tableBoxStyle: CSSProperties = {
  border: '1px solid #e9ecf2',
  borderRadius: '0.5rem',
  marginBottom: '0.5rem',
  width: '90%',
  padding: '0.5rem',
};

export const loaderContainer: CSSProperties = {
  display: 'flex',
  flexDirection: 'column',
  alignItems: 'center',
  justifyContent: 'center',
  height: '100%',
};
