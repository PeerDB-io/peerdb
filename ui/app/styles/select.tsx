import { Theme } from 'react-select';

export default function SelectTheme(theme: Theme) {
  return {
    ...theme,
    colors: {
      ...theme.colors,
      primary25: 'rgba(48, 164, 108, 0.3)',
      primary: 'rgba(48, 164, 108, 0.3)',
    },
  };
}
