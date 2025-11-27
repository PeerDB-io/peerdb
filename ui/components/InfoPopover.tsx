'use client';

import { Icon } from '@/lib/Icon';
import * as Popover from '@radix-ui/react-popover';
import { useTheme } from 'styled-components';

export default function InfoPopover({
  tips,
  link,
  command,
}: {
  tips: string;
  link?: string;
  command?: string;
}) {
  const theme = useTheme();

  return (
    <Popover.Root modal={true}>
      <Popover.Trigger asChild>
        <button className='IconButton' aria-label='Update dimensions'>
          <Icon name='info' />
        </button>
      </Popover.Trigger>

      <Popover.Portal>
        <Popover.Content
          style={{ backgroundColor: theme.colors.base.surface.normal }}
          className='PopoverContent'
          sideOffset={5}
        >
          <div
            style={{
              border: `1px solid ${theme.colors.base.border.normal}`,
              boxShadow: '0 0 10px rgba(0,0,0,0.15)',
              padding: '0.5rem',
              borderRadius: '0.5rem',
              minWidth: '15rem',
            }}
          >
            {tips.split('.').map((sentence, index) => (
              <p className='Text' style={{ fontSize: 16 }} key={index}>
                {sentence.trim()}
              </p>
            ))}

            {link && (
              <a
                href={link}
                rel='noreferrer'
                target='_blank'
                style={{ color: theme.colors.blue.fill.normal, fontSize: 16 }}
              >
                Click here for more info.
              </a>
            )}

            {command && (
              <div
                style={{
                  display: 'flex',
                  flexDirection: 'column',
                  marginTop: '0.5rem',
                }}
              >
                <p>
                  You can use the below command to create a publication for all
                  tables:
                </p>
                <pre style={{ fontSize: 16 }}>
                  <code>{command}</code>
                </pre>
              </div>
            )}
          </div>
        </Popover.Content>
      </Popover.Portal>
    </Popover.Root>
  );
}
