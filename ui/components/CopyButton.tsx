'use client';
import { Button } from '@/lib/Button';
import { Icon } from '@/lib/Icon';
import { useState } from 'react';

export const CopyButton = ({ text }: { text: string }) => {
  const [copied, setCopied] = useState(false);
  const handleClick = () => {
    navigator.clipboard.writeText(text);
    setCopied(true);
  };
  return (
    <Button
      onClick={handleClick}
      style={{ backgroundColor: copied ? 'lightgreen' : 'auto' }}
    >
      <Icon name={copied ? 'check' : 'content_copy'} />
    </Button>
  );
};
