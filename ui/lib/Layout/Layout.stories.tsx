import { Meta, StoryObj } from '@storybook/react';
import { Button } from '../Button';
import { Header } from '../Header';
import { Icon } from '../Icon';
import { Label } from '../Label';
import { Sidebar, SidebarItem } from '../Sidebar';
import { Layout } from './Layout';

export default {
  title: 'Components / Layout',
  component: Layout,
  tags: ['autodocs'],
  args: {},
} satisfies Meta<typeof Layout>;

type Story = StoryObj<typeof Layout>;
export const Overview: Story = {
  render: () => (
    <Layout
      sidebar={
        <Sidebar
          topTitle={<Label variant='headline'>PeerDB</Label>}
          bottomRow={
            <>
              <Button>Help and Support</Button>
              <Button>Log out</Button>
            </>
          }
          bottomLabel={<Label variant='footnote'>App. v0.7.0</Label>}
        >
          <SidebarItem leadingIcon={<Icon name='widgets' />}>
            Dashboard
          </SidebarItem>
          <SidebarItem leadingIcon={<Icon name='cable' />}>
            Connectors
          </SidebarItem>
          <SidebarItem leadingIcon={<Icon name='compare_arrows' />}>
            Mirrors
          </SidebarItem>
          <SidebarItem leadingIcon={<Icon name='cloud' />}>Cloud</SidebarItem>
          <SidebarItem leadingIcon={<Icon name='settings' />}>
            Settings
          </SidebarItem>
        </Sidebar>
      }
    >
      <Header variant='title2'>Dashboard</Header>
    </Layout>
  ),
};
