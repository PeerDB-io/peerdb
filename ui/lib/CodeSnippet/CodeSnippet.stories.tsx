import { Meta, StoryObj } from '@storybook/react';
import { CodeSnippet } from './CodeSnippet';

export default {
  title: 'Components / Input / CodeSnippet',
  component: CodeSnippet,
  tags: ['autodocs'],
  args: {
    defaultValue: `apiVersion: apps/v1
    kind: DaemonSet
    metadata:
      name: kernel-optimization
      namespace: kube-system
      labels:
        tier: management
        app: kernel-optimization
  spec:
    selector:`,
    disabled: false,
  },
} satisfies Meta<typeof CodeSnippet>;

type Story = StoryObj<typeof CodeSnippet>;

export const Default: Story = {};
export const Disabled: Story = {
  args: {
    disabled: true,
  },
};
