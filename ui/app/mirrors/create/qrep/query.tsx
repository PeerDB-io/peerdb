import Editor from '@monaco-editor/react';
import { Dispatch, SetStateAction } from 'react';
const options = {
  readOnly: false,
  minimap: { enabled: false },
  fontSize: 14,
};

interface QueryProps {
  setter: Dispatch<SetStateAction<string>>;
  query: string;
}
const QRepQuery = (props: QueryProps) => {
  return (
    <Editor
      options={options}
      height='10vh'
      value={props.query}
      defaultLanguage='pgsql'
      defaultValue='-- Query for QRep Mirror'
      onChange={(value) => props.setter(value as string)}
    />
  );
};

export default QRepQuery;
