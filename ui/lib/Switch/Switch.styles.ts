import * as RadixSwitch from '@radix-ui/react-switch';
import { styled } from 'styled-components';

export const SwitchRoot = styled(RadixSwitch.Root)`
  --background-color: ${({ theme }) => theme.colors.base.fill.normal};

  all: unset;

  width: 40px;
  height: 24px;

  position: relative;
  background-color: var(--background-color);
  border-radius: ${({ theme }) => theme.radius.xxLarge};
  margin: ${({ theme }) => `${theme.spacing.xSmall} ${theme.spacing.medium}`};

  &:hover {
    --background-color: ${({ theme }) => theme.colors.base.fill.hovered};
  }

  &:focus {
    outline: none;
  }

  &:focus-visible {
    outline: 2px solid ${({ theme }) => theme.colors.accent.border.normal};
  }

  &[data-disabled] {
    opacity: 0.3;
  }

  &[data-state='checked'] {
    --background-color: ${({ theme }) => theme.colors.positive.fill.normal};
  }

  &[data-state='checked']:hover {
    --background-color: ${({ theme }) => theme.colors.positive.fill.hovered};
  }
`;

export const SwitchThumb = styled(RadixSwitch.Thumb)`
  display: block;
  width: 20px;
  height: 20px;
  border-radius: 50%;

  background-color: ${({ theme }) => theme.colors.base.surface.normal};

  transition: translate 100ms;
  translate: 2px 0;

  &[data-state='checked'] {
    translate: 18px;
  }
`;
