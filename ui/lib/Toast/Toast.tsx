'use client';
import * as RadixToast from '@radix-ui/react-toast';
import { RenderObject } from '../types';
import { isDefined } from '../utils/isDefined';
import { renderObjectWith } from '../utils/renderObjectWith';
import {
  ToastAction,
  ToastContent,
  ToastRoot,
  ToastTitle,
  ToastViewport,
} from './Toast.styles';

type ToastProps = RadixToast.ToastProps & {
  message?: string;
  icon?: RenderObject;
  actionText?: string;
  className?: string;
};

/**
 * Toast component
 *
 * [Figma spec](https://www.figma.com/file/DBMDh1LNNvp9H99N9lZgJ7/PeerDB?type=design&node-id=1-1949&mode=design&t=GFIJ9vbM0Q2bR3Ml-4)
 *
 * Based on the [Radix Toast](https://www.radix-ui.com/primitives/docs/components/toast) component
 */
export function Toast({
  message,
  icon,
  actionText: action,
  children,
  ...rootProps
}: ToastProps) {
  const Icon = renderObjectWith(icon);
  const Action = isDefined(action) && (
    <ToastAction altText={action}>{action}</ToastAction>
  );
  const Message = isDefined(message) && <ToastTitle>{message}</ToastTitle>;

  return (
    <RadixToast.Provider swipeDirection='right'>
      <ToastRoot {...rootProps}>
        <ToastContent>
          {Icon}
          {Message}
          {children}
        </ToastContent>
        {Action}
      </ToastRoot>
      <ToastViewport />
    </RadixToast.Provider>
  );
}
