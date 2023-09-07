import * as RadixSelect from '@radix-ui/react-select';
import styled, { css } from 'styled-components';

export const SelectContent = styled(RadixSelect.SelectContent)`
  padding: ${({ theme }) => theme.spacing.medium};
  border: 1px solid ${({ theme }) => theme.colors.base.border.subtle};
  border-radius: ${({ theme }) => theme.radius.medium};
  background-color: ${({ theme }) => theme.colors.base.background.normal};
  ${({ theme }) => css(theme.dropShadow.large)};

  width: var(--radix-popper-anchor-width, 'auto');
`;
