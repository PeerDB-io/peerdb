'use client';
import { CSSProperties, ReactNode } from 'react';
import { useTheme } from 'styled-components';

// Layout helpers used only by the create-mirror page.

export function Band({
  title,
  description,
  children,
  first,
}: {
  title: string;
  description?: ReactNode;
  children: ReactNode;
  first?: boolean;
}) {
  const theme = useTheme();
  return (
    <section
      style={{
        display: 'grid',
        gridTemplateColumns: '240px minmax(0, 1fr)',
        columnGap: '3rem',
        padding: '1.75rem 0',
        borderTop: first ? 'none' : `1px solid ${theme.colors.base.border.subtle}`,
      }}
    >
      <div>
        <h2
          style={{
            margin: 0,
            fontSize: 15,
            fontWeight: 600,
            letterSpacing: '-0.01em',
            color: theme.colors.base.text.highContrast,
          }}
        >
          {title}
        </h2>
        {description && (
          <p
            style={{
              margin: '0.4rem 0 0',
              fontSize: 13.5,
              lineHeight: 1.55,
              color: theme.colors.base.text.lowContrast,
            }}
          >
            {description}
          </p>
        )}
      </div>
      <div style={{ minWidth: 0 }}>{children}</div>
    </section>
  );
}

export function SettingRow({
  label,
  description,
  control,
}: {
  label: ReactNode;
  description?: ReactNode;
  control: ReactNode;
}) {
  const theme = useTheme();
  return (
    <div
      style={{
        display: 'grid',
        gridTemplateColumns: 'minmax(0, 1fr) auto',
        columnGap: '2rem',
        alignItems: 'center',
        padding: '1rem 0',
        borderBottom: `1px solid ${theme.colors.base.border.subtle}`,
      }}
    >
      <div>
        <div
          style={{
            fontSize: 14,
            fontWeight: 500,
            color: theme.colors.base.text.highContrast,
          }}
        >
          {label}
        </div>
        {description && (
          <div
            style={{
              marginTop: 2,
              maxWidth: 560,
              fontSize: 13,
              lineHeight: 1.5,
              color: theme.colors.base.text.lowContrast,
            }}
          >
            {description}
          </div>
        )}
      </div>
      <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
        {control}
      </div>
    </div>
  );
}

export function StatusDot({ ready, label }: { ready: boolean; label: string }) {
  const theme = useTheme();
  const color = ready
    ? theme.colors.accent.text.lowContrast
    : theme.colors.base.text.lowContrast;
  return (
    <span
      style={{
        display: 'inline-flex',
        alignItems: 'center',
        gap: 6,
        fontSize: 12.5,
        fontWeight: 500,
        color,
      }}
    >
      <i
        style={{
          width: 7,
          height: 7,
          borderRadius: '50%',
          display: 'inline-block',
          background: ready
            ? theme.colors.accent.fill.normal
            : theme.colors.base.fill.normal,
          boxShadow: ready
            ? `0 0 0 3px ${theme.colors.accent.surface.normal}`
            : 'none',
        }}
      />
      {label}
    </span>
  );
}

export const connectorKeyframes = `
@keyframes pdbflow { to { stroke-dashoffset: -44; } }
.pdb-flow { stroke-dasharray: 2 9; animation: pdbflow 3s linear infinite; }
@media (prefers-reduced-motion: reduce) { .pdb-flow { animation: none; } }
`;

export const actionBarStyle = (
  theme: ReturnType<typeof useTheme>
): CSSProperties => ({
  position: 'sticky',
  bottom: '1rem',
  zIndex: 5,
  marginTop: '2rem',
  display: 'flex',
  alignItems: 'center',
  columnGap: '1.25rem',
  padding: '0.75rem 1rem 0.75rem 1.5rem',
  background: theme.colors.base.background.normal,
  border: `1px solid ${theme.colors.base.border.subtle}`,
  borderRadius: '0.9rem',
  boxShadow: '0 12px 32px -12px rgba(0, 0, 0, 0.3)',
});
