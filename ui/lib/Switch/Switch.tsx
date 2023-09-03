import { ComponentProps } from 'react';
import { SwitchRoot, SwitchThumb } from './Switch.styles';

type SwitchProps = ComponentProps<typeof SwitchRoot>;
/**
 * Switch inputfield
 *
 * [Figma spec](https://www.figma.com/file/DBMDh1LNNvp9H99N9lZgJ7/PeerDB?type=design&node-id=1-1328&mode=design&t=b6qwokbWgkPCcgVS-4)
 */
export function Switch({ ...switchRootProps }: SwitchProps) {
  return (
    <SwitchRoot {...switchRootProps}>
      <SwitchThumb />
    </SwitchRoot>
  );
}
