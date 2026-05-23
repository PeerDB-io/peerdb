import { appThemeColors } from '@/lib/AppTheme/appThemeColors';
import { appThemeColorsDark } from '@/lib/AppTheme/appThemeColorsDark';
import { useTheme } from '@/lib/AppTheme/ThemeContext';
import { useMemo } from 'react';
import { Theme } from 'react-select';

export function useSelectTheme() {
  const { theme: appTheme } = useTheme();

  return useMemo(() => {
    return (theme: Theme) => {
      const c = appTheme === 'dark' ? appThemeColorsDark : appThemeColors;

      return {
        ...theme,
        colors: {
          ...theme.colors,
          primary: c.accent.fill.normal,
          primary75: c.accent.fill.hovered,
          primary50: c.accent.surface.hovered,
          primary25: c.accent.surface.normal,
          danger: c.destructive.fill.normal,
          dangerLight: c.destructive.surface.normal,
          neutral0: c.base.background.normal,
          neutral5: c.base.surface.normal,
          neutral10: c.base.border.subtle,
          neutral20: c.base.border.normal,
          neutral30: c.base.border.hovered,
          neutral40: c.base.fill.normal,
          neutral50: c.base.text.lowContrast,
          neutral60: c.base.fill.hovered,
          neutral70: c.base.text.lowContrast,
          neutral80: c.base.text.highContrast,
          neutral90: c.base.text.highContrast,
        },
      };
    };
  }, [appTheme]);
}
