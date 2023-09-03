import { ComponentProps } from 'react';
import { BaseTextArea } from './CodeSnippet.styles';

type CodeSnipperProps = ComponentProps<'textarea'>;

export function CodeSnippet({ ...textAreaProps }: CodeSnipperProps) {
  return (
    <BaseTextArea autoCorrect='false' spellCheck={false} {...textAreaProps} />
  );
}
