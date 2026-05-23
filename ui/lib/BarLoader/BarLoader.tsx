'use client';

import { ComponentProps } from 'react';
import { BarLoader as ReactBarLoader } from 'react-spinners';

const defaultBarLoaderColor = '#36d7b7';

export function BarLoader({
  color = defaultBarLoaderColor,
  ...props
}: ComponentProps<typeof ReactBarLoader>) {
  return <ReactBarLoader color={color} {...props} />;
}
