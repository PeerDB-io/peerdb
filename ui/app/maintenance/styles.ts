import { Icon } from '@/lib/Icon';
import styled, { keyframes } from 'styled-components';

export const EmptyActivitiesIcon = styled(Icon)`
  margin-bottom: 0.5rem;
`;

export const StatusRow = styled.div`
  display: flex;
  align-items: center;
  gap: 0.5rem;
`;

export const PhaseList = styled.div`
  display: flex;
  flex-direction: column;
  gap: 0.75rem;
`;

export const PhaseCard = styled.div<{ $active: boolean }>`
  padding: 0.75rem;
  border: 1px solid
    ${({ $active }) => ($active ? 'oklch(87.1% 0.15 154.449)' : 'oklch(92.8% 0.006 264.531)')};
  border-radius: 0.5rem;
  background-color: ${({ $active }) => ($active ? 'oklch(96.2% 0.044 156.743)' : 'oklch(98.5% 0.002 247.839)')};
  color: ${({ $active }) => ($active ? 'oklch(44.8% 0.119 151.328)' : 'oklch(44.6% 0.03 256.802)')};
  transition:
    color 0.15s,
    background-color 0.15s,
    border-color 0.15s;
`;

export const StatusIcon = styled(Icon)<{
  $status: 'success' | 'error' | 'warning' | 'running';
}>`
  color: ${({ $status }) =>
    ({
      success: 'oklch(62.7% 0.194 149.214)',
      error: 'oklch(57.7% 0.245 27.325)',
      warning: 'oklch(68.1% 0.162 75.834)',
      running: 'oklch(64.6% 0.222 41.116)',
    })[$status]};
`;

export const ActivitiesTable = styled.table`
  width: 100%;
  border-collapse: collapse;
  border: 1px solid oklch(87.2% 0.01 258.338);

  th,
  td {
    border: 1px solid oklch(87.2% 0.01 258.338);
    padding: 0.5rem 1rem;
  }

  th {
    text-align: left;
  }

  thead,
  tbody tr:hover {
    background-color: oklch(98.5% 0.002 247.839);
  }

  td:first-child {
    font-family: var(--font-mono);
    font-size: 0.875rem;
    line-height: 1.25rem;
  }
`;

export const HeartbeatPayload = styled.div`
  overflow: hidden;
  padding: 0.5rem;
  border-radius: 0.25rem;
  background-color: oklch(96.7% 0.003 264.542);
  font-family: var(--font-mono);
  font-size: 0.875rem;
  line-height: 1.25rem;
  text-overflow: ellipsis;
  white-space: nowrap;
`;

export const SkipWaitPanel = styled.div`
  padding: 1rem;
  border: 1px solid oklch(94.5% 0.129 101.54);
  border-radius: 0.5rem;
  background-color: oklch(98.7% 0.026 102.212);
`;

export const FlowNameInput = styled.input`
  width: 100%;
  padding: 0.5rem 0.75rem;
  border: 1px solid oklch(87.2% 0.01 258.338);
  border-radius: 0.375rem;

  &:focus {
    border-color: transparent;
    outline: 2px solid transparent;
    outline-offset: 2px;
    box-shadow: 0 0 0 2px ${({ theme }) => theme.colors.accent.fill.normal};
  }
`;

const spin = keyframes`
  to {
    transform: rotate(360deg);
  }
`;

export const SpinningIcon = styled(Icon)`
  animation: ${spin} 1s linear infinite;
`;

export const SignalResult = styled.div<{ $success: boolean }>`
  margin-top: 0.75rem;
  padding: 0.75rem;
  border: 1px solid
    ${({ $success }) => ($success ? 'oklch(92.5% 0.084 155.995)' : 'oklch(88.5% 0.062 18.334)')};
  border-radius: 0.375rem;
  background-color: ${({ $success }) => ($success ? 'oklch(96.2% 0.044 156.743)' : 'oklch(93.6% 0.032 17.717)')};
  color: ${({ $success }) => ($success ? 'oklch(44.8% 0.119 151.328)' : 'oklch(44.4% 0.177 26.899)')};
`;

export const StatusError = styled(StatusRow)`
  margin-top: 1rem;
  padding: 1rem;
  border: 1px solid oklch(80.8% 0.114 19.571);
  border-radius: 0.5rem;
  background-color: oklch(93.6% 0.032 17.717);
`;

export const StatusBanner = styled(StatusRow)<{ $running: boolean }>`
  padding: 1rem;
  border: 1px solid
    ${({ $running }) => ($running ? 'oklch(70.5% 0.213 47.604)' : 'oklch(72.3% 0.219 149.579)')};
  border-radius: 0.5rem;
  background-color: ${({ $running }) => ($running ? 'oklch(95.4% 0.038 75.164)' : 'oklch(96.2% 0.044 156.743)')};
`;
