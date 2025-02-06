'use client';
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
  return <Editor options={options} value={config} language='json' />;
}
