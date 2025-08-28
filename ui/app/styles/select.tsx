import { Theme } from 'react-select';
import { appThemeColors } from '@/lib/AppTheme/appThemeColors';
import { appThemeColorsDark } from '@/lib/AppTheme/appThemeColorsDark';

// Create theme function based on whether we're in dark mode
export default function SelectTheme(theme: Theme) {
  // Check if the page is in dark mode based on the root element's class
  const isDark = typeof window !== 'undefined' && 
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
