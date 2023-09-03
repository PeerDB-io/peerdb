import { styled } from 'styled-components';

export const SlotWrapper = styled.div`
  padding: ${({ theme }) => `${theme.spacing.xSmall} ${theme.spacing.medium}`};
  background-color: ${({ theme }) => theme.colors.base.background.subtle};
  border: 1px solid ${({ theme }) => theme.colors.base.background.subtle};
`;
