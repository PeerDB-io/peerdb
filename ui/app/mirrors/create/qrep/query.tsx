import { Label } from '@/lib/Label';
import Editor from '@monaco-editor/react';
const options = {
  readOnly: false,
  minimap: { enabled: false },
  fontSize: 14,
};

interface QueryProps {
  setter: (val: string) => void;
  query: string;
}
const QRepQuery = (props: QueryProps) => {
  return (
    <>
      <Label as='label' style={{ marginTop: '1rem', marginBottom: '1rem' }}>
        Replication Query
      </Label>
      <Editor
        options={options}
        height='10vh'
        value={props.query}
        defaultLanguage='pgsql'
        defaultValue='-- Query for QRep Mirror'
        onChange={(value) => props.setter(value as string)}
      />
    </>
  );
};

export default QRepQuery;
