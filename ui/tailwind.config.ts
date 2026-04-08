import type { Config } from 'tailwindcss';
import { appThemeColors } from './lib/AppTheme/appThemeColors';

module.exports = {
  darkMode: 'class',
  content: [
    './app/**/*.{js,ts,jsx,tsx,mdx}',
    './components/**/*.{js,ts,jsx,tsx,mdx}',
    './lib/**/*.{js,ts,jsx,tsx,mdx}',
  ],
  theme: {
    extend: {
      colors: {
        ...appThemeColors,
        blue: {
          500: appThemeColors.accent.fill.normal,
        },
        gray: {
          500: appThemeColors.base.border.subtle,
        },
      },
    },
  },
  plugins: [require('tailwindcss-animate')],
} satisfies Config;
