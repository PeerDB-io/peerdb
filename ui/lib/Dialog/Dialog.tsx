'use client';
import * as RadixDialog from '@radix-ui/react-dialog';
import { PropsWithChildren } from 'react';
import { RenderObject } from '../types';
import { isDefined } from '../utils/isDefined';
import { DialogContent, DialogContentProps } from './DialogContent';

type DialogProps = RadixDialog.DialogProps & {
  triggerButton: RenderObject;
} & PropsWithChildren &
  DialogContentProps;

export function Dialog({
  triggerButton,
  size,
  children,
  ...rootProps
}: DialogProps) {
  const TriggerButton = isDefined(triggerButton) && triggerButton;

  return (
    <RadixDialog.Root {...rootProps}>
      <RadixDialog.Trigger asChild>{TriggerButton}</RadixDialog.Trigger>
      <RadixDialog.Portal>
        <DialogContent size={size}>{children}</DialogContent>
      </RadixDialog.Portal>
    </RadixDialog.Root>
  );
}

type DialogCloseProps = PropsWithChildren;
/** Wrapps a component allowing click on the child to close the parent Dialog */
export function DialogClose({ children }: DialogCloseProps) {
  return <RadixDialog.Close asChild>{children}</RadixDialog.Close>;
}
