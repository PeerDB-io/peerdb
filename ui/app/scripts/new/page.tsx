'use client';
import { ScriptsType } from '@/app/dto/ScriptsDTO';
import { notifyErr } from '@/app/utils/notify';
import PeerDBCodeEditor from '@/components/PeerDBEditor';
import { Button } from '@/lib/Button';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label/Label';
import { ProgressCircle } from '@/lib/ProgressCircle';
import { TextField } from '@/lib/TextField';
import Link from 'next/link';
import { useRouter, useSearchParams } from 'next/navigation';
import { useState } from 'react';
import { ToastContainer } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';
import { AddScript, HandleEditScript } from '../handlers';

const EditScript = () => {
  const params = useSearchParams();
  const router = useRouter();
  const scriptStringBase64 = params.get('script');
  let script: ScriptsType = {
    id: 1,
    name: '',
    lang: 'lua',
    source: `
-- This is a sample script
-- Fill in the onRecord function to transform the incoming record
local json = require "json"

function onRecord(r)
  return json.encode(r.row)
end
    `,
  };
  let inEditMode: boolean = false;
  if (scriptStringBase64) {
    const scriptString = Buffer.from(scriptStringBase64, 'base64').toString(
      'utf-8'
    );
    script = JSON.parse(scriptString);
    inEditMode = true;
  }

  const [newScript, setNewScript] = useState<ScriptsType>(script);
  const [loading, setLoading] = useState(false);
  const handleAdd = (script?: ScriptsType) => {
    if (!script || !script.source) {
      notifyErr('Empty scripts not allowed');
      return;
    }
    if (!script?.name) {
      notifyErr('Please enter a script name');
      return;
    }
    setLoading(true);
    AddScript(script).then((success) => {
      setLoading(false);
      if (success) {
        router.replace('/scripts');
      }
    });
  };

  const handleEdit = (script?: ScriptsType) => {
    if (!script || !script.source) {
      notifyErr('Empty scripts not allowed');
      return;
    }
    if (!script?.name) {
      notifyErr('Please enter a script name');
      return;
    }
    setLoading(true);
    HandleEditScript(script).then((success) => {
      setLoading(false);
      if (success) {
        router.replace('/scripts');
      }
    });
  };

  return (
    <div
      style={{
        display: 'flex',
        flexDirection: 'column',
        rowGap: '1rem',
        padding: '2rem',
      }}
    >
      <Button
        as={Link}
        aria-label='icon-button'
        style={{ width: 'fit-content' }}
        href={'/scripts'}
      >
        <Icon name='chevron_left' />{' '}
      </Button>
      <Label variant='title3'>{inEditMode ? script?.name : 'New script'}</Label>

      <div
        style={{
          display: 'flex',
          flexDirection: 'column',
          rowGap: '1rem',
          width: '100%',
        }}
      >
        <div>
          <Label as='label' style={{ display: 'block' }}>
            You can write a Lua script here for specifying message structure for
            your topics.
          </Label>
          <Label>
            You can find the
            <Label
              as={Link}
              target='_blank'
              style={{
                color: 'teal',
                cursor: 'pointer',
                width: 'fit-content',
              }}
              href={`https://docs.peerdb.io/lua/reference`}
            >
              PeerDB Lua API reference here.
            </Label>
          </Label>
          <Label as='label' style={{ display: 'block' }}>
            Here is an
            <Label
              as={Link}
              target='_blank'
              style={{
                color: 'teal',
                cursor: 'pointer',
                width: 'fit-content',
              }}
              href={`https://github.com/PeerDB-io/examples/blob/main/debezium.lua`}
            >
              example script for a Debezium-like format
            </Label>
            for your Kafka messages.
          </Label>
        </div>
        <div>
          <Label>Script Name</Label>
          <TextField
            onChange={(e) =>
              setNewScript((prev) => ({ ...prev, name: e.target.value }))
            }
            variant='simple'
            defaultValue={newScript?.name}
            style={{ height: '2rem', width: '30%' }}
          />
        </div>

        <div style={{ width: '100%', resize: 'vertical', overflow: 'auto' }}>
          <Label as='label' style={{ marginBottom: '1rem' }}>
            Script Code
          </Label>
          <PeerDBCodeEditor
            setter={(newQuery: string) =>
              setNewScript((prev) => ({ ...prev, source: newQuery }))
            }
            code={newScript?.source}
            height={'30vh'}
            language='lua'
          />
        </div>
      </div>
      <Button
        variant='normalSolid'
        style={{ width: '8em', height: '2.5em' }}
        onClick={() => {
          inEditMode ? handleEdit(newScript) : handleAdd(newScript);
        }}
      >
        {loading ? (
          <ProgressCircle variant='determinate_progress_circle' />
        ) : inEditMode ? (
          'Update script'
        ) : (
          'Add script'
        )}
      </Button>
      <ToastContainer />
    </div>
  );
};

export default EditScript;
