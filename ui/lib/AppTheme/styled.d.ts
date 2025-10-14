import 'styled-components';
import { appTheme } from './appTheme';
import { appThemeColors } from './appThemeColors';

// Use the exact structure from appThemeColors
type ThemeColors = typeof appThemeColors;

declare module 'styled-components' {
  export interface DefaultTheme extends Omit<typeof appTheme, 'colors'> {
    colors: ThemeColors;
  }
}
