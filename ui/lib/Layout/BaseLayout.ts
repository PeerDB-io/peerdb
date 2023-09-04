import type { RenderObject } from '../types';

export type BaseLayoutProps = {
  label: RenderObject;
  description?: RenderObject;
  instruction?: RenderObject;
  action?: RenderObject;
  className?: string;
};
