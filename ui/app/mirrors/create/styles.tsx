import { CSSProperties } from 'styled-components';
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

export { MirrorButtonContainer, MirrorButtonStyle };
