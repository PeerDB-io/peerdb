'use client';
import { TrackerProps as TremorTrackerChartProps } from '@tremor/react';
import { RenderObject } from '../types';
import { renderObjectWith } from '../utils/renderObjectWith';
import { StyledTrackerChart, StyledWrapper } from './TrackerChart.styles';

type TrackerChartProps = {
  top?: RenderObject;
} & TremorTrackerChartProps;

/**
 * Tracker chart component
 *
 * Thin wrapper around the [Tremor TrackerChart component](https://www.tremor.so/docs/components/tracker-chart)
 */
export function TrackerChart({ top, ...TrackerChartProps }: TrackerChartProps) {
  const TopWrapper = renderObjectWith(top);
  return (
    <StyledWrapper>
      {TopWrapper}
      <StyledTrackerChart {...TrackerChartProps} />
    </StyledWrapper>
  );
}
