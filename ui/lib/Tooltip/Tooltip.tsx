import * as RadixTooltip from '@radix-ui/react-tooltip';
import { PropsWithChildren } from 'react';
import { TooltipContent, TriggerContent } from './Tooltip.styles';

type TooltipProps = PropsWithChildren<{
  content: string;
}> &
  RadixTooltip.TooltipProps &
  RadixTooltip.TooltipContentProps;

export function Tooltip({
  content,
  children,
  open,
  defaultOpen,
  onOpenChange,
  ...contentProps
}: TooltipProps) {
  return (
    <RadixTooltip.Provider>
      <RadixTooltip.Root
        open={open}
        disableHoverableContent
        defaultOpen={defaultOpen}
        onOpenChange={onOpenChange}
      >
        <RadixTooltip.Trigger asChild>
          <TriggerContent style={{ display: 'inline-block' }}>
            {children}
          </TriggerContent>
        </RadixTooltip.Trigger>
        <TooltipContent {...contentProps}>{content}</TooltipContent>
      </RadixTooltip.Root>
    </RadixTooltip.Provider>
  );
}
