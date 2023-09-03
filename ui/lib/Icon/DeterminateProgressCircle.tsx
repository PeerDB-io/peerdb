import { SVGProps } from 'react';

export function DeterminateProgressCircle({
  ...svgProps
}: SVGProps<SVGSVGElement>) {
  return (
    <svg
      xmlns='http://www.w3.org/2000/svg'
      width='24'
      height='24'
      viewBox='0 0 24 24'
      fill='none'
      {...svgProps}
    >
      <circle
        opacity='0.12'
        cx='12'
        cy='12'
        r='9'
        stroke='currentColor'
        stroke-width='2'
      />
      <path
        d='M12 3C13.4203 3 14.8204 3.33614 16.0859 3.98094C17.3514 4.62574 18.4463 5.56089 19.2812 6.70993C20.116 7.85897 20.667 9.18928 20.8892 10.5921C21.1114 11.9949 20.9984 13.4304 20.5595 14.7812C20.1206 16.1319 19.3683 17.3597 18.364 18.364C17.3597 19.3683 16.1319 20.1206 14.7812 20.5595C13.4304 20.9984 11.9949 21.1114 10.5921 20.8892C9.18929 20.667 7.85898 20.116 6.70994 19.2812'
        stroke='currentColor'
        stroke-width='2'
      />
    </svg>
  );
}
