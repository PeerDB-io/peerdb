import { CSSProperties } from 'react';
const MirrorButtonStyle: CSSProperties = {
  width: '10em',
  height: '3em',
  boxShadow: '0 2px 4px rgba(0, 0, 0, 0.2)',
  borderRadius: '2em',
  fontWeight: 'bold',
};

const MirrorButtonContainer: CSSProperties = {
  height: '2rem',
  display: 'flex',
  alignItems: 'center',
  columnGap: '1rem',
  position: 'fixed',
  bottom: '5%',
  right: '5%',
};

const MirrorCardStyle: CSSProperties = {
  cursor: 'pointer',
  padding: '0.5rem',
  flex: '1 0 0',
  display: 'flex',
  flexDirection: 'column',
  justifyContent: 'space-between',
  border: '2px solid rgba(0, 0, 0, 0.07)',
  borderRadius: '1rem',
};

// Styles for the redesigned create page. The exports above are still used by
// the edit-mirror page, so they stay as they are.
const CreateButtonStyle: CSSProperties = {
  height: '2.75rem',
  padding: '0 1.25rem',
  borderRadius: '0.6rem',
  fontWeight: 500,
  whiteSpace: 'nowrap',
};

export {
  CreateButtonStyle,
  MirrorButtonContainer,
  MirrorButtonStyle,
  MirrorCardStyle,
};
