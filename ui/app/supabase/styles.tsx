import { useTheme as useStyledTheme } from 'styled-components';

export const ProjectsContainerStyle: React.CSSProperties = {
  width: '100%',
  height: '100%',
  display: 'flex',
  flexDirection: 'column',
  alignItems: 'center',
  justifyContent: 'center',
  rowGap: '1rem',
};

export const ProjectListStyle: React.CSSProperties = {
  display: 'flex',
  flexDirection: 'column',
  rowGap: '1rem',
  maxHeight: '40%',
  overflowY: 'auto',
  width: '30%',
};

export function useProjectCardStyle(): React.CSSProperties {
  const theme = useStyledTheme();
  return {
    display: 'flex',
    flexDirection: 'column',
    alignItems: 'flex-start',
    fontSize: 13,
    padding: '0.5rem',
    borderRadius: '1rem',
    backgroundColor: theme.colors.base.surface.normal,
    boxShadow: `0px 0px 2px ${theme.colors.base.border.subtle}`,
    border: `1px solid ${theme.colors.base.border.normal}`,
    cursor: 'pointer',
    width: '100%',
  };
}

export const ProjectNameStyle: React.CSSProperties = {
  width: '70%',
  padding: '4px 8px',
  alignItems: 'start',
  textAlign: 'left',
  overflowX: 'hidden',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
};
