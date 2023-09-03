import { Tracker } from '@tremor/react';
import { styled } from 'styled-components';

export const StyledWrapper = styled.div`
  display: flex;
  flex-direction: column;
`;

export const StyledTrackerChart = styled(Tracker)`
  padding: ${({ theme }) => theme.spacing.medium};
  height: ${({ theme }) => theme.size.xxSmall};
  column-gap: ${({ theme }) => theme.spacing.medium};
`;
