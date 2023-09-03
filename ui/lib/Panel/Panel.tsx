import styled from 'styled-components';

export const Panel = styled.div`
  padding: ${({ theme }) => theme.spacing.medium};
  display: flex;
  flex-flow: column;
`;
Panel.displayName = 'Panel';
