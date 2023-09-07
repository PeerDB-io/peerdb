import * as RadixProgress from '@radix-ui/react-progress';
import styled from 'styled-components';

export const ProgressWrapper = styled.div`
  display: flex;
  align-items: center;
  margin: ${({ theme }) => `${theme.spacing.xSmall} ${theme.spacing.medium}`};
  height: ${({ theme }) => theme.spacing.xxLarge};
`;

export const ProgressRoot = styled(RadixProgress.Root)`
  position: relative;
  overflow: hidden;
  background-color: ${({ theme }) => theme.colors.base.border.subtle};
  border-radius: ${({ theme }) => theme.radius.xSmall};
  width: 100%;
  height: ${({ theme }) => theme.spacing.xSmall};
`;

type ProgressIndicatorProps = {
  $progress: number;
};
export const ProgressIndicator = styled(
  RadixProgress.Indicator
)<ProgressIndicatorProps>`
  background-color: ${({ theme }) => theme.colors.accent.fill.normal};
  width: 100%;
  height: 100%;
  translate: -${({ $progress }) => 100 - $progress}% 0%;
  transition: translate 660ms cubic-bezier(0.65, 0, 0.35, 1);
`;
