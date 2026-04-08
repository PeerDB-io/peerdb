'use client';

import { useTheme } from '@/lib/AppTheme/ThemeContext';
import Editor from '@monaco-editor/react';

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
  const { theme } = useTheme();

  return (
    <Editor
      options={props.options ?? defaultOptions}
      height={props.height ?? '10vh'}
      value={props.code}
      defaultLanguage={props.language ?? 'pgsql'}
      onChange={(value) => props.setter(value as string)}
      theme={theme === 'dark' ? 'vs-dark' : 'vs'}
    />
  );
}
