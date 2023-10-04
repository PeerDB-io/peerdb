import { Icon } from '@/lib/Icon';
import * as Popover from '@radix-ui/react-popover';
export const InfoPopover = ({
  tips,
  link,
}: {
  tips: string;
  link?: string;
}) => {
  return (
    <Popover.Root modal={true}>
      <Popover.Trigger asChild>
        <button className='IconButton' aria-label='Update dimensions'>
          <Icon name='info' />
        </button>
      </Popover.Trigger>

      <Popover.Portal>
        <Popover.Content
          style={{ position: 'absolute' }}
          className='PopoverContent'
          side='right'
          sideOffset={5}
        >
          <div
            style={{
              border: '1px solid #d9d7d7',
              boxShadow: '0 0 10px #d9d7d7',
              padding: '0.5rem',
              borderRadius: '0.5rem',
              minWidth: '15rem',
            }}
          >
            <p className='Text' style={{ fontSize: 13 }}>
              {tips}
            </p>

            {link && (
              <a
                href={link}
                rel='noreferrer'
                target='_blank'
                style={{ color: '#0070f3', fontSize: 13 }}
              >
                Click here for more info.
              </a>
            )}
          </div>
        </Popover.Content>
      </Popover.Portal>
    </Popover.Root>
  );
};
