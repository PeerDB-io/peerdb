import { TrackerProps as TremorTrackerChartProps } from '@tremor/react';
import { RenderSlot } from '../types';
import { renderSlotWith } from '../utils/renderSlotWith';
import { StyledTrackerChart, StyledWrapper } from './TrackerChart.styles';

type TrackerChartProps = {
  top?: RenderSlot;
} & TremorTrackerChartProps;

/**
 * Tracker chart component
 *
 * Thin wrapper around the [Tremor TrackerChart component](https://www.tremor.so/docs/components/tracker-chart)
 */
export function TrackerChart({ top, ...TrackerChartProps }: TrackerChartProps) {
  const TopWrapper = renderSlotWith(top);
  return (
    <StyledWrapper>
      {TopWrapper}
      <StyledTrackerChart {...TrackerChartProps} />
    </StyledWrapper>
  );
}
