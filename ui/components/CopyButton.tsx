'use client';
import { Button } from '@/lib/Button';
import { Icon } from '@/lib/Icon';
import { useCallback, useState } from 'react';

export default function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false);
  const handleClick = useCallback(() => {
    navigator.clipboard.writeText(text);
    setCopied(true);
  }, [text]);
  return (
    <Button
      variant='normalBorderless'
      onClick={handleClick}
      style={{
        backgroundColor: copied ? 'rgba(48, 164, 108,0.3)' : 'auto',
        marginLeft: '0.5rem',
      }}
    >
      <Icon name={copied ? 'check' : 'content_copy'} />
    </Button>
  );
}
