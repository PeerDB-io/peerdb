import { Meta, StoryObj } from '@storybook/react';
import { Icon } from '../Icon';
import { SidebarItem } from './SidebarItem';

export default {
  title: 'Components / Sidebar / SidebarItem',
  component: SidebarItem,
  tags: ['autodocs'],
  args: {
    selected: false,
    disabled: false,
    leadingIcon: () => <Icon name='draft' />,
    trailingIcon: () => <Icon name='star' />,
    suffix: 'Suffix',
  },
} as Meta<typeof SidebarItem>;

type Story = StoryObj<typeof SidebarItem>;
export const Default: Story = {
  render: (props) => <SidebarItem {...props}>Label</SidebarItem>,
};

export const Selected: Story = {
  args: {
    selected: true,
  },
  render: (props) => <SidebarItem {...props}>Label</SidebarItem>,
};

export const Disabled: Story = {
  args: {
    disabled: true,
  },
  render: (props) => <SidebarItem {...props}>Label</SidebarItem>,
};
