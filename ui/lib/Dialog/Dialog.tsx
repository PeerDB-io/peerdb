'use client';
import * as RadixDialog from '@radix-ui/react-dialog';
import { VisuallyHidden } from '@radix-ui/react-visually-hidden';
import { PropsWithChildren } from 'react';
import { RenderObject } from '../types';
import { isDefined } from '../utils/isDefined';
import { DialogContent, DialogContentProps } from './DialogContent';

type DialogProps = RadixDialog.DialogProps & {
  triggerButton: RenderObject;
  noInteract: boolean;
  title?: string;
} & PropsWithChildren &
  DialogContentProps;

export function Dialog({
  triggerButton,
  size,
  children,
  noInteract,
  ...rootProps
}: DialogProps) {
  const TriggerButton = isDefined(triggerButton) && triggerButton;

  return (
    <RadixDialog.Root {...rootProps}>
      <RadixDialog.Trigger asChild>{TriggerButton}</RadixDialog.Trigger>
      <RadixDialog.Portal>
        <DialogContent
          onPointerDownOutside={(e) => {
            if (noInteract) e.preventDefault();
          }}
          size={size}
          style={{
            position: 'fixed',
            left: '50%',
            top: '50%',
            transform: 'translate(-0%, -50%)',
          }}
        >
          <VisuallyHidden>
            <RadixDialog.Title>{rootProps.title ?? 'Dialog'}</RadixDialog.Title>
          </VisuallyHidden>
          {children}
        </DialogContent>
      </RadixDialog.Portal>
    </RadixDialog.Root>
  );
}

type DialogCloseProps = PropsWithChildren;
/** Wrapps a component allowing click on the child to close the parent Dialog */
export function DialogClose({ children }: DialogCloseProps) {
  return <RadixDialog.Close asChild>{children}</RadixDialog.Close>;
}
