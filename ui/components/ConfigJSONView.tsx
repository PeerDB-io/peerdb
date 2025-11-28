'use client';
import { useTheme } from '@/lib/AppTheme/ThemeContext';
import Editor from '@monaco-editor/react';
import { editor } from 'monaco-editor';

const options: editor.IStandaloneEditorConstructionOptions = {
  readOnly: true,
  minimap: { enabled: false },
  fontSize: 14,
  lineNumbers: 'off',
  scrollBeyondLastLine: false,
};

export default function ConfigJSONView({ config }: { config: string }) {
  const { theme } = useTheme();
  return (
    <Editor
      options={options}
      value={config}
      language='json'
      theme={theme === 'dark' ? 'vs-dark' : 'vs'}
    />
  );
}
