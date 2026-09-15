import { TableRow } from '@/lib/Table';
import * as Dialog from '@radix-ui/react-dialog';
import styled from 'styled-components';

export const ColumnOverlay = styled(Dialog.Overlay)`
  position: fixed;
  inset: 0;
  z-index: 50;
`;

export const ColumnContent = styled(Dialog.Content)`
  position: fixed;
  top: 50%;
  left: 50%;
  translate: -50% -50%;
  z-index: 50;
  width: 100%;
  max-width: 56rem;
  max-height: 80vh;
  overflow: hidden;
  border-radius: 0.5rem;
  background-color: white;
  box-shadow:
    0 20px 25px -5px rgb(0 0 0 / 10%),
    0 8px 10px -6px rgb(0 0 0 / 10%);

  .dark & {
    background-color: oklch(21% 0.034 264.665);
  }
`;

export const ColumnHeader = styled.div`
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 1.5rem;
  border-bottom: 1px solid oklch(92.8% 0.006 264.531);
`;

export const ColumnBody = styled.div`
  height: 30em;
  max-height: 60vh;
  overflow: auto;
  padding: 1.5rem;
`;

export const ColumnMessage = styled.div`
  padding-block: 2rem;
  text-align: center;
`;

export const ColumnRow = styled(TableRow)<{ $excluded: boolean }>`
  ${({ $excluded }) =>
    $excluded &&
    `
      opacity: 0.6;
      background-color: oklch(98.5% 0.002 247.839);

      .dark & {
        background-color: oklch(27.8% 0.033 256.848);
      }

      > td:nth-child(-n + 4) {
        text-decoration-line: line-through;
      }
    `}
`;

export const ExcludedColumnNote = styled.div`
  margin-top: 1rem;
  padding-top: 1rem;
  border-top: 1px solid oklch(92.8% 0.006 264.531);
  font-size: 0.75rem;
  line-height: 1rem;
  color: #e2e2e2;

  .dark & {
    color: oklch(70.7% 0.022 261.325);
  }
`;

export const ColumnFooter = styled.div`
  display: flex;
  justify-content: flex-end;
  padding: 1.5rem;
  border-top: 1px solid oklch(92.8% 0.006 264.531);
`;
