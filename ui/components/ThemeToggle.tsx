'use client';

import { useTheme } from '@/lib/AppTheme';
import styled from 'styled-components';

const ToggleButton = styled.button`
  position: relative;
  width: 56px;
  height: 28px;
  border-radius: 14px;
  background-color: ${({ theme }) => theme.colors.base.surface.normal};
  border: 1px solid ${({ theme }) => theme.colors.base.border.normal};
  cursor: pointer;
  transition: all 0.3s ease;
  padding: 2px;
  display: flex;
  align-items: center;

  &:hover {
    background-color: ${({ theme }) => theme.colors.base.surface.hovered};
  }

  &:focus {
    outline: 2px solid ${({ theme }) => theme.colors.accent.border.normal};
    outline-offset: 2px;
  }
`;

const ToggleSlider = styled.div<{ $isDark: boolean }>`
  position: absolute;
  width: 22px;
  height: 22px;
  border-radius: 50%;
  background-color: ${({ theme }) => theme.colors.accent.fill.normal};
  transition: transform 0.3s ease;
  transform: translateX(${({ $isDark }) => ($isDark ? '30px' : '2px')});
  display: flex;
  align-items: center;
  justify-content: center;
`;

const IconWrapper = styled.span`
  font-size: 14px;
  color: ${({ theme }) => theme.colors.special.fixed.white};
  user-select: none;
`;

export function ThemeToggle() {
  const { theme, toggleTheme } = useTheme();
  const isDark = theme === 'dark';

  return (
    <ToggleButton
      onClick={toggleTheme}
      aria-label={`Switch to ${isDark ? 'light' : 'dark'} mode`}
      title={`Switch to ${isDark ? 'light' : 'dark'} mode`}
    >
      <ToggleSlider $isDark={isDark}>
        <IconWrapper>{isDark ? '🌙' : '☀️'}</IconWrapper>
      </ToggleSlider>
    </ToggleButton>
  );
}
