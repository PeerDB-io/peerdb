import { ComponentProps } from 'react';
import { BaseInputField, BaseTextArea } from './TextField.styles';

type TextFieldProps =
  | ({
      variant: 'simple';
    } & ComponentProps<typeof BaseInputField>)
  | ({
      variant: 'text-area';
    } & ComponentProps<typeof BaseTextArea>);

/**
 * TextField inputfield
 *
 * [Figma spec](https://www.figma.com/file/DBMDh1LNNvp9H99N9lZgJ7/PeerDB?type=design&node-id=1-246&mode=design&t=b6qwokbWgkPCcgVS-4)
 */
export function TextField({ variant, ...inputProps }: TextFieldProps) {
  if (variant === 'text-area') {
    return (
      <BaseTextArea {...(inputProps as ComponentProps<typeof BaseTextArea>)} />
    );
  }
  return (
    <BaseInputField
      {...(inputProps as ComponentProps<typeof BaseInputField>)}
    />
  );
}
