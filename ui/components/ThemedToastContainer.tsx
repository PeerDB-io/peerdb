'use client';
import { useTheme } from '@/lib/AppTheme/ThemeContext';
import { ToastContainer, ToastContainerProps } from 'react-toastify';

export default function ThemedToastContainer(props: ToastContainerProps) {
  const { theme } = useTheme();
  return <ToastContainer theme={theme === 'dark' ? 'dark' : 'light'} {...props} />;
}
