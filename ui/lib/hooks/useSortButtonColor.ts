'use client';
import { useTheme as useStyledTheme } from 'styled-components';

export function useSortButtonColor() {
  const theme = useStyledTheme();
  return (active: boolean) =>
    active
      ? theme.colors.positive.text.lowContrast
      : theme.colors.base.text.lowContrast;
}
