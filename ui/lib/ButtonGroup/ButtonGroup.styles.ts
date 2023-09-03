import { styled } from 'styled-components';

export const BaseButtonGroup = styled.div`
  display: flex;
  flex-flow: row wrap;
  gap: ${({ theme }) => theme.spacing.medium};
`;
