import styled from 'styled-components';

export const StyledWrapper = styled.aside`
  padding: ${({ theme }) => theme.spacing.medium};
  display: flex;
  flex-direction: column;
  justify-content: space-between;
  align-items: flex-start;
  background-color: ${({ theme }) => theme.colors.base.background.subtle};
  border-right: 1px solid ${({ theme }) => theme.colors.base.border.subtle};
  height: 100vh;
`;

export const StyledItemWrapper = styled.div`
  flex: 1 1 auto;
  width: 100%;
  display: flex;
  flex-flow: column;
`;

export const BottomRowWrapper = styled.div`
  width: 100%;
  display: flex;
  flex-direction: column;
`;
