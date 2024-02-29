import { Theme } from 'react-select';

const SelectTheme = (theme: Theme) => ({
  ...theme,
  colors: {
    ...theme.colors,
    primary25: 'rgba(48, 164, 108, 0.5)',
    primary: 'rgba(48, 164, 108, 0.5)',
  },
});

export default SelectTheme;
