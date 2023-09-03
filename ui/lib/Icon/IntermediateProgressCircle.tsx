import type { SVGProps } from 'react';

export function IntermediateProgressCircle({
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
      <rect
        x='10.75'
        y='2'
        width='2.5'
        height='6.5'
        rx='1.25'
        fill='currentColor'
      />
      <rect
        opacity='0.48'
        x='10.75'
        y='15.5'
        width='2.5'
        height='6.5'
        rx='1.25'
        fill='currentColor'
      />
      <rect
        opacity='0.12'
        x='18.187'
        y='4.04517'
        width='2.5'
        height='6.5'
        rx='1.25'
        transform='rotate(45 18.187 4.04517)'
        fill='currentColor'
      />
      <rect
        opacity='0.84'
        x='4.04492'
        y='5.81274'
        width='2.5'
        height='6.5'
        rx='1.25'
        transform='rotate(-45 4.04492 5.81274)'
        fill='currentColor'
      />
      <rect
        opacity='0.36'
        x='13.5908'
        y='15.3586'
        width='2.5'
        height='6.5'
        rx='1.25'
        transform='rotate(-45 13.5908 15.3586)'
        fill='currentColor'
      />
      <rect
        opacity='0.72'
        x='2'
        y='13.25'
        width='2.5'
        height='6.5'
        rx='1.25'
        transform='rotate(-90 2 13.25)'
        fill='currentColor'
      />
      <rect
        opacity='0.24'
        x='15.5'
        y='13.25'
        width='2.5'
        height='6.5'
        rx='1.25'
        transform='rotate(-90 15.5 13.25)'
        fill='currentColor'
      />
      <rect
        opacity='0.6'
        x='8.64111'
        y='13.5911'
        width='2.5'
        height='6.5'
        rx='1.25'
        transform='rotate(45 8.64111 13.5911)'
        fill='currentColor'
      />
    </svg>
  );
}
