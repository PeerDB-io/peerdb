import * as RadixToast from '@radix-ui/react-toast';
import styled, { keyframes } from 'styled-components';

export const ToastAction = styled(RadixToast.Action)`
  all: unset;
  cursor: pointer;

  display: flex;
  justify-content: center;
  align-items: center;
  padding: ${({ theme }) => `${theme.spacing.medium} ${theme.spacing.large}`};
  border-left: 1px solid ${({ theme }) => theme.colors.base.border.normal};
  ${({ theme }) => theme.text.medium.body};

  &:hover {
    background-color: ${({ theme }) => theme.colors.base.surface.hovered};
  }

  &:focus {
    outline: none;
  }

  &:focus-visible {
    outline: 2px solid ${({ theme }) => theme.colors.accent.border.normal};
    outline-offset: -2px;
  }
`;

export const ToastTitle = styled(RadixToast.Title)`
  ${({ theme }) => theme.text.regular.body};
`;

const VIEWPORT_PADDING = 25;

const hide = keyframes({
  '0%': { opacity: 1 },
  '100%': { opacity: 0 },
});

const slideIn = keyframes({
  from: { transform: `translateX(calc(100% + ${VIEWPORT_PADDING}px))` },
  to: { transform: 'translateX(0)' },
});

const swipeOut = keyframes({
  from: { transform: 'translateX(var(--radix-toast-swipe-end-x))' },
  to: { transform: `translateX(calc(100% + ${VIEWPORT_PADDING}px))` },
});

export const ToastRoot = styled(RadixToast.Root)`
  background-color: ${({ theme }) => theme.colors.base.background.normal};
  border-radius: ${({ theme }) => theme.radius.medium};
  border: 1px solid ${({ theme }) => theme.colors.base.border.normal};
  ${({ theme }) => theme.dropShadow.large};

  display: inline-flex;
  flex-flow: row nowrap;

  &[data-state='open'] {
    animation: ${slideIn} 150ms cubic-bezier(0.16, 1, 0.3, 1);
  }

  &[data-state='closed'] {
    animation: ${hide} 100ms ease-in;
  }

  &[data-swipe='move'] {
    transform: translateX(var(--radix-toast-swipe-move-x));
  }

  &[data-swipe='cancel'] {
    transform: translateX(0);
    transition: transform 200ms ease-out;
  }

  &[data-swipe='end'] {
    animation: ${swipeOut} 100ms ease-out;
  }
`;

export const ToastContent = styled.div`
  display: flex;
  flex-flow: row nowrap;
  align-items: center;
  column-gap: ${({ theme }) => theme.spacing.medium};
  padding: ${({ theme }) => `${theme.spacing.medium} ${theme.spacing.large}`};
`;

export const ToastViewport = styled(RadixToast.Viewport)`
  position: fixed;
  top: 0;
  right: 0;
  display: flex;
  flex-direction: column;
  padding: ${VIEWPORT_PADDING};
  gap: 10;
  width: 390;
  max-width: 100vw;
  margin: 0;
  list-style: none;
  z-index: 2147483647;
  outline: none;
`;
