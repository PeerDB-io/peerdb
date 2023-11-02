import styled from 'styled-components';

export const StyledWrapper = styled.div`
  display: flex;
  flex-direction: column;
  width: 100%;
`;

export const TableWrapper = styled.div`
  width: 100%;
  overflow-x: auto;
`;

export const StyledTable = styled.table`
  width: 100%;
  border-spacing: 0;
  border-collapse: collapse;
`;

export const StyledTableBody = styled.tbody``;

export const StyledTableHeader = styled.thead`
  text-align: left;
`;

export const ToolbarWrapper = styled.div`
  display: flex;
  flex-flow: row wrap;
  justify-content: space-between;
`;

export const ToolbarSlot = styled.div`
  display: flex;
  flex-flow: row nowrap;
  column-gap: ${({ theme }) => theme.spacing.medium};
`;
