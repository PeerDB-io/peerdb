'use client';

import Editor from '@monaco-editor/react';
import { useEffect, useState } from 'react';

const defaultOptions = {
  readOnly: false,
  minimap: { enabled: false },
  fontSize: 14,
};

interface CodeEditorProps {
  setter: (value: string) => void;
  code?: string;
  language?: string;
  height?: string;
  options?: Object;
}

export default function PeerDBCodeEditor(props: CodeEditorProps) {
  const [theme, setTheme] = useState<'light' | 'vs-dark'>('light');

  useEffect(() => {
    // Check if the page is in dark mode
    const isDark = document.documentElement.classList.contains('dark');
    setTheme(isDark ? 'vs-dark' : 'light');

    // Watch for theme changes
    const observer = new MutationObserver(() => {
      const isDark = document.documentElement.classList.contains('dark');
      setTheme(isDark ? 'vs-dark' : 'light');
    });

    observer.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ['class'],
    });

    return () => observer.disconnect();
  }, []);

  return (
    <Editor
      options={props.options ?? defaultOptions}
      height={props.height ?? '10vh'}
      value={props.code}
      defaultLanguage={props.language ?? 'pgsql'}
      onChange={(value) => props.setter(value as string)}
      theme={theme}
    />
  );
}
