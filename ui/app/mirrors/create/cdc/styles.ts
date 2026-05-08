import { CSSProperties } from 'react';
import type { DefaultTheme } from 'styled-components';

export const expandableStyle = {
  fontSize: 14,
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'space-between',
  cursor: 'pointer',
};

export const schemaBoxStyle = (theme: DefaultTheme): CSSProperties => ({
  width: '100%',
  marginTop: '0.5rem',
  padding: '0.5rem',
  display: 'flex',
  flexDirection: 'column',
  border: `1px solid ${theme.colors.base.border.subtle}`,
  borderRadius: '0.8rem',
});

export const tableBoxStyle = (theme: DefaultTheme): CSSProperties => ({
  border: `1px solid ${theme.colors.base.border.subtle}`,
  borderRadius: '0.5rem',
  marginBottom: '0.5rem',
  width: '90%',
  padding: '0.5rem',
});

export const loaderContainer: CSSProperties = {
  display: 'flex',
  flexDirection: 'column',
  alignItems: 'center',
  justifyContent: 'center',
  height: '100%',
};

export const tooltipStyle = (theme: DefaultTheme): CSSProperties => ({
  width: '100%',
  backgroundColor: theme.colors.base.surface.normal,
  color: theme.colors.base.text.highContrast,
  padding: '0.5rem',
});

export const columnBoxDividerStyle: CSSProperties = {
  marginTop: '1.5rem',
  marginLeft: 0,
  marginBottom: '0.5rem',
  width: '90%',
  opacity: 0.5,
};

export const sortingKeyPillStyle = (theme: DefaultTheme): CSSProperties => ({
  display: 'flex',
  columnGap: '0.3rem',
  alignItems: 'center',
  border: `1px solid ${theme.colors.base.border.subtle}`,
  borderRadius: '1rem',
  paddingLeft: '0.5rem',
  paddingRight: '0.5rem',
});

export const sortingKeyPillContainerStyle: CSSProperties = {
  display: 'flex',
  marginTop: '0.5rem',
  columnGap: '0.5rem',
  rowGap: '0.5rem',
  alignItems: 'center',
  flexWrap: 'wrap',
};

const targetHeight = 30;

export const engineOptionStyles = {
  control: (base: any) => ({
    ...base,
    minHeight: 'initial',
    fontSize: 12,
  }),
  menu: (base: any) => ({
    ...base,
    fontSize: 12,
  }),
  valueContainer: (base: any) => ({
    ...base,
    height: `${targetHeight - 1 - 1}px`,
    padding: '0 8px',
  }),
  clearIndicator: (base: any) => ({
    ...base,
    padding: `${(targetHeight - 20 - 1 - 1) / 2}px`,
  }),
  dropdownIndicator: (base: any) => ({
    ...base,
    padding: `${(targetHeight - 20 - 1 - 1) / 2}px`,
  }),
};
