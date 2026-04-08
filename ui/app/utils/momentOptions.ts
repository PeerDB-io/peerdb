import { argThresholdOpts } from 'moment';

export const humanizeThresholds: argThresholdOpts = {
  ss: 1, // show seconds instead of "a few seconds" after 1 second
  s: 60, // show seconds up to 60 seconds (instead of "a minute" at 45s)
  m: 60, // show minutes up to 60 minutes (instead of "an hour" at 45m)
  h: 24, // show hours up to 24 hours (instead of "a day" at 22h)
  d: 30, // show days up to 30 days (instead of "a month" at 26d)
  M: 12, // show months up to 12 months (instead of "a year" at ~11m)
};
