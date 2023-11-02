'use client';
import { ComponentProps } from 'react';
import { BaseTextArea } from './CodeSnippet.styles';

type CodeSnipperProps = ComponentProps<'textarea'>;

export function CodeSnippet({ ref: _ref, ...textAreaProps }: CodeSnipperProps) {
  return (
    <BaseTextArea autoCorrect='false' spellCheck={false} {...textAreaProps} />
  );
}
