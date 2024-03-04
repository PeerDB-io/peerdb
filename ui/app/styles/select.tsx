import { Theme } from 'react-select';

const SelectTheme = (theme: Theme) => ({
  ...theme,
  colors: {
    ...theme.colors,
    primary25: 'rgba(48, 164, 108, 0.3)',
    primary: 'rgba(48, 164, 108, 0.3)',
  },
});

export default SelectTheme;
