import { appThemeColors } from '@/lib/AppTheme/appThemeColors';
import { appThemeColorsDark } from '@/lib/AppTheme/appThemeColorsDark';
import { useTheme } from '@/lib/AppTheme/ThemeContext';
import { useMemo } from 'react';
import { Theme } from 'react-select';

// Hook version for components that need reactive theme updates
export function useSelectTheme() {
  const { theme: appTheme } = useTheme();

  return useMemo(() => {
    return (theme: Theme) => {
      const colors = appTheme === 'dark' ? appThemeColorsDark : appThemeColors;

      return {
        ...theme,
        colors: {
          ...theme.colors,
          primary: colors.select.primary,
          primary25: colors.select.primary25,
          neutral0: colors.select.neutral0,
          neutral80: colors.select.neutral80,
        },
      };
    };
  }, [appTheme]);
}

// Static function for backward compatibility
// Checks DOM at render time
function SelectTheme(theme: Theme) {
  const isDark =
    typeof window !== 'undefined' &&
    document.documentElement.classList.contains('dark');

  const colors = isDark ? appThemeColorsDark : appThemeColors;

  return {
    ...theme,
    colors: {
      ...theme.colors,
      primary: colors.select.primary,
      primary25: colors.select.primary25,
      neutral0: colors.select.neutral0,
      neutral80: colors.select.neutral80,
    },
  };
}

export default SelectTheme;
