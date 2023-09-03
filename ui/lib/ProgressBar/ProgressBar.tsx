import {
  ProgressIndicator,
  ProgressRoot,
  ProgressWrapper,
} from './ProgressBar.styles';

type ProgressBarProps = {
  /** Progress value: 0 - 100 */
  progress: number;

  className?: string;
};

/**
 * Progress bar
 *
 * [Figma spec](https://www.figma.com/file/DBMDh1LNNvp9H99N9lZgJ7/PeerDB?type=design&node-id=1-1406)
 */
export function ProgressBar({ progress, ...wrapperProps }: ProgressBarProps) {
  const clampedProgress = Math.min(100, Math.max(0, progress));

  return (
    <ProgressWrapper {...wrapperProps}>
      <ProgressRoot value={clampedProgress}>
        <ProgressIndicator $progress={clampedProgress} />
      </ProgressRoot>
    </ProgressWrapper>
  );
}
