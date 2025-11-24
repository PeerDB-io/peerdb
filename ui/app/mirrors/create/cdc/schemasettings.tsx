'use client';
import { Icon } from '@/lib/Icon';
import * as Popover from '@radix-ui/react-popover';
import { useState } from 'react';
import { useTheme as useStyledTheme } from 'styled-components';

export default function SchemaSettings({
  schema,
  setTargetSchemaOverride,
}: {
  schema: string;
  setTargetSchemaOverride: (schema: string) => void;
}) {
  const styledTheme = useStyledTheme();
  const [inputValue, setInputValue] = useState(schema);
  const [savedIndicator, setSavedIndicator] = useState(false);

  const handleInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setInputValue(e.target.value);
  };

  const handleSave = () => {
    setTargetSchemaOverride(inputValue);
    setSavedIndicator(true);
    setTimeout(() => setSavedIndicator(false), 3000);
  };

  return (
    <Popover.Root modal={true}>
      <Popover.Trigger asChild>
        <div style={{ alignSelf: 'center', cursor: 'pointer' }}>
          <Icon name='settings' />
        </div>
      </Popover.Trigger>

      <Popover.Portal>
        <Popover.Content
          style={{ backgroundColor: styledTheme.colors.base.surface.normal }}
          className='PopoverContent'
          sideOffset={5}
        >
          <div
            style={{
              border: `1px solid ${styledTheme.colors.base.border.normal}`,
              boxShadow: `0 0 10px ${styledTheme.colors.base.border.subtle}`,
              padding: '0.5rem',
              borderRadius: '0.5rem',
              minWidth: '15rem',
            }}
          >
            <h3 style={{ fontSize: 14 }}>Schema On Target</h3>
            <input
              type='text'
              value={inputValue}
              onChange={handleInputChange}
              style={{
                width: '100%',
                padding: '0.5rem',
                marginBottom: '0.5rem',
                borderRadius: '0.25rem',
                border: `1px solid ${styledTheme.colors.base.border.normal}`,
                backgroundColor: styledTheme.colors.base.background.normal,
                color: styledTheme.colors.base.text.highContrast,
              }}
            />
            <button
              onClick={handleSave}
              style={{
                padding: '0.25rem 0.5rem',
                backgroundColor: styledTheme.colors.accent.fill.normal,
                color: styledTheme.colors.special.fixed.white,
                border: 'none',
                borderRadius: '0.25rem',
                cursor: 'pointer',
                fontSize: 14,
              }}
            >
              Save
            </button>
            {savedIndicator && (
              <span style={{ marginLeft: '0.5rem', color: styledTheme.colors.positive.text.lowContrast }}>
                success
              </span>
            )}
          </div>
        </Popover.Content>
      </Popover.Portal>
    </Popover.Root>
  );
}
