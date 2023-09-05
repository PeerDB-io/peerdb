import * as RadixTooltip from '@radix-ui/react-tooltip';
import styled, { css } from 'styled-components';

export const TooltipContent = styled(RadixTooltip.Content)`
  display: inline-flex;
  padding: ${({ theme }) => `${theme.spacing.xSmall} ${theme.spacing.medium}`};
  flex-direction: column;
  justify-content: center;
  align-items: center;
  border-radius: ${({ theme }) => theme.radius.xxSmall};
  background: ${({ theme }) => theme.colors.special.inverted.black};

  ${({ theme }) => css(theme.dropShadow.large)}
  user-select: none;

  color: ${({ theme }) => theme.colors.special.inverted.white};
  ${({ theme }) => css(theme.text.regular.footnote)};

  width: var(--radix-tooltip-trigger-width);
  max-height: var(--radix-tooltip-content-available-height);
`;

export const TriggerContent = styled.div`
  display: inline-block;
`;
