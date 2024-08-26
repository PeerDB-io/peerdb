'use client';
import { useState } from 'react';

export default function RestPage() {
  const [method, setMethod] = useState('POST');
  const [url, setUrl] = useState('');
  const [body, setBody] = useState('');
  const [text, setText] = useState('');

  return (
    <div>
      <input
        style={{
          display: 'block',
        }}
        value={url}
        onInput={(e: React.ChangeEvent<HTMLInputElement>) =>
          setUrl(e.target.value)
        }
      />
      <div style={{ display: 'flex', justifyContent: 'space-between' }}>
        {method}&emsp;
        {['GET', 'POST', 'PUT', 'DELETE'].map((m) => (
          <input key={m} type='button' value={m} onClick={() => setMethod(m)} />
        ))}
      </div>
      <textarea
        style={{ width: '100%' }}
        value={body}
        onInput={(e: React.ChangeEvent<HTMLTextAreaElement>) =>
          setBody(e.target.value)
        }
      />
      <input
        style={{
          display: 'block',
        }}
        type='button'
        value='fetch'
        onClick={() =>
          fetch(url, {
            method: method,
            body: body,
          })
            .then((res) => res.text())
            .then((text) => setText(text))
        }
      />
      <div style={{ fontFamily: 'monospace' }}>{text}</div>
    </div>
  );
}
