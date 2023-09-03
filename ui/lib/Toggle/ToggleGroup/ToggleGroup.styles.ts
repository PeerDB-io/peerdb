import * as RadixToggleGroup from '@radix-ui/react-toggle-group';
import { styled } from 'styled-components';
import { baseButtonStyle } from '../styles';

export const BaseToggleGroupRoot = styled(RadixToggleGroup.Root)`
  display: inline-flex;
  flex-flow: row nowrap;
`;

export const ToggleGroupItem = styled(RadixToggleGroup.Item)`
  ${baseButtonStyle}
`;
ToggleGroupItem.displayName = 'ToggleGroupItem';
