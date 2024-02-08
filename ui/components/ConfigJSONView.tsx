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

const ConfigJSONView = ({ config }: { config: string }) => {
  return <Editor options={options} value={config} language='json' />;
};

export default ConfigJSONView;
