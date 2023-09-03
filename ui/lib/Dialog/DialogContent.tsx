import * as RadixDialog from '@radix-ui/react-dialog';
import { PropsWithChildren } from 'react';
import { BaseContent, DialogSize } from './DialogContent.styles';

export type DialogContentProps = PropsWithChildren<{
  size: DialogSize;
}> &
  RadixDialog.DialogContentProps;
export function DialogContent({
  children,
  size,
  ...contentProps
}: DialogContentProps) {
  return (
    <BaseContent {...contentProps} $size={size}>
      {children}
    </BaseContent>
  );
}
