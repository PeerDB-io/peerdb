'use client';
import { CSSProperties, Dispatch, ReactNode, SetStateAction, useState } from 'react';
import ReactSelect from 'react-select';
import styled, { useTheme as useStyledTheme } from 'styled-components';

import { TableMapRow } from '@/app/dto/MirrorsDTO';
import { useSelectTheme } from '@/app/styles/select';
import { notifySortingKey } from '@/app/utils/notify';
import ThemedToastContainer from '@/components/ThemedToastContainer';
import { ColumnSetting } from '@/grpc_generated/flow';
import { ColumnsItem } from '@/grpc_generated/route';
import { Icon, IconProps } from '@/lib/Icon';
import { TextField } from '@/lib/TextField';
import { engineOptionStyles } from './styles';

type KeyField = 'ordering' | 'partitioning';
type PartitionMode = 'none' | 'columns' | 'expr';
type Tab = 'columns' | 'keys' | 'engine';

interface Props {
  enabled: boolean;
  isClickHouse: boolean;
  row: TableMapRow;
  columns: ColumnsItem[] | undefined;
  setRows: Dispatch<SetStateAction<TableMapRow[]>>;
  engineOptions: { value: string; label: string }[];
  engineValue: string;
  onEngine: (value: string) => void;
  onPartitionKey: (value: string) => void;
  onShardingKey: (value: string) => void;
  onPolicyName: (value: string) => void;
  onPartitionByExpr: (value: string) => void;
  children: ReactNode;
}

// Ordered key names for one table (ordering or partitioning), 1-based in the proto.
export function keyNames(row: TableMapRow, field: KeyField): string[] {
  return row.columns
    .filter((c) => c[field] > 0)
    .sort((a, b) => a[field] - b[field])
    .map((c) => c.sourceName);
}

// Rewrites `field` on this table's columns so `names` get 1..n and the rest 0.
export function withKeys(
  row: TableMapRow,
  field: KeyField,
  names: string[]
): TableMapRow {
  const columns: ColumnSetting[] = row.columns.map((c) => ({
    ...c,
    [field]: names.indexOf(c.sourceName) + 1,
  }));
  for (const name of names) {
    if (!columns.some((c) => c.sourceName === name)) {
      columns.push({
        sourceName: name,
        destinationName: '',
        destinationType: '',
        ordering: 0,
        partitioning: 0,
        nullableEnabled: false,
        [field]: names.indexOf(name) + 1,
      });
    }
  }
  return { ...row, columns };
}

export default function TableSettingsTabs(props: Props) {
  const { enabled, isClickHouse, row, columns, setRows, children } = props;
  const theme = useStyledTheme();
  const selectTheme = useSelectTheme();
  const [tab, setTab] = useState<Tab>('columns');
  const sortKeys = keyNames(row, 'ordering');
  const partKeys = keyNames(row, 'partitioning');
  const [mode, setMode] = useState<PartitionMode>(
    row.partitionByExpr ? 'expr' : partKeys.length ? 'columns' : 'none'
  );
  if (!enabled) return <>{children}</>;

  const mut = theme.colors.base.text.lowContrast;
  const line = theme.colors.base.border.subtle;
  const included = (columns ?? []).filter((c) => !row.exclude.has(c.name));
  const typeOf = (name: string) =>
    columns?.find((c) => c.name === name)?.type ?? '';
  const pkNames = (columns ?? []).filter((c) => c.isKey).map((c) => c.name);
  const customTypes = row.columns.filter((c) => c.destinationType).length;

  const setKeys = (field: KeyField, names: string[]) =>
    setRows((rows) =>
      rows.map((r) => (r.source === row.source ? withKeys(r, field, names) : r))
    );
  const move = (names: string[], i: number, d: number) => {
    const next = [...names];
    [next[i], next[i + d]] = [next[i + d], next[i]];
    return next;
  };

  const choosePartition = (m: PartitionMode) => {
    setMode(m);
    if (m !== 'expr' && row.partitionByExpr) props.onPartitionByExpr('');
    if (m !== 'columns' && partKeys.length) setKeys('partitioning', []);
  };

  const hint: CSSProperties = { fontSize: 12.5, color: mut, margin: '4px 0 0' };
  const h4: CSSProperties = { fontSize: 14, fontWeight: 600, margin: 0 };
  const label: CSSProperties = {
    display: 'block',
    fontSize: 13,
    fontWeight: 500,
    marginBottom: 6,
  };

  const keyList = (field: KeyField, names: string[], addLabel: string) => (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 6, marginTop: 12 }}>
      {names.map((name, i) => (
        <div
          key={name}
          style={{
            display: 'grid',
            gridTemplateColumns: '24px minmax(0,1fr) auto auto',
            alignItems: 'center',
            gap: 10,
            minHeight: 40,
            padding: '0 6px 0 10px',
            border: `1px solid ${line}`,
            borderRadius: 8,
            fontSize: 13,
          }}
        >
          <span
            style={{
              display: 'inline-flex',
              justifyContent: 'center',
              alignItems: 'center',
              width: 22,
              height: 22,
              borderRadius: 6,
              background: theme.colors.base.text.highContrast,
              color: theme.colors.base.background.normal,
              fontSize: 11.5,
              fontWeight: 600,
            }}
          >
            {i + 1}
          </span>
          <span>
            {name}
            {pkNames.includes(name) && <PkBadge />}
          </span>
          <span style={{ color: mut, fontSize: 12.5 }}>{typeOf(name)}</span>
          <span style={{ display: 'flex' }}>
            <IconButton
              label={`Move ${name} up`}
              icon='arrow_upward'
              disabled={row.editingDisabled || i === 0}
              onClick={() => setKeys(field, move(names, i, -1))}
            />
            <IconButton
              label={`Move ${name} down`}
              icon='arrow_downward'
              disabled={row.editingDisabled || i === names.length - 1}
              onClick={() => setKeys(field, move(names, i, 1))}
            />
            <IconButton
              label={`Remove ${name}`}
              icon='close'
              disabled={row.editingDisabled}
              onClick={() =>
                setKeys(
                  field,
                  names.filter((n) => n !== name)
                )
              }
            />
          </span>
        </div>
      ))}
      <ReactSelect
        aria-label={addLabel}
        placeholder={`+ ${addLabel}`}
        isDisabled={row.editingDisabled}
        value={null}
        styles={engineOptionStyles}
        theme={selectTheme}
        options={included
          .filter((c) => !names.includes(c.name))
          .map((c) => ({ value: c.name, label: c.name }))}
        onChange={(opt) => {
          if (!opt) return;
          if (field === 'ordering' && names.length === 0) notifySortingKey();
          setKeys(field, [...names, opt.value]);
        }}
      />
    </div>
  );

  const orderBy = sortKeys.length ? sortKeys : pkNames;
  const partitionBy = row.partitionByExpr || partKeys.join(', ');
  const ddl = [
    `ENGINE = ${props.engineOptions.find((o) => o.value === props.engineValue)?.label ?? ''}`,
    `ORDER BY (${orderBy.join(', ') || 'tuple()'})`,
    partitionBy ? `PARTITION BY (${partitionBy})` : '',
  ]
    .filter(Boolean)
    .join('\n');

  const radio = (m: PartitionMode, title: string, desc: string, extra?: ReactNode) => (
    <div
      style={{
        padding: '12px 14px',
        border: `1px solid ${mode === m ? theme.colors.accent.border.normal : line}`,
        background: mode === m ? theme.colors.accent.background.subtle : undefined,
        borderRadius: 8,
        fontSize: 13,
      }}
    >
      <label style={{ display: 'flex', gap: 10, alignItems: 'flex-start', cursor: 'pointer' }}>
      <input
        type='radio'
        name={`partition-${row.source}`}
        checked={mode === m}
        disabled={row.editingDisabled}
        onChange={() => choosePartition(m)}
        style={{ margin: '2px 0 0', accentColor: theme.colors.accent.fill.normal }}
      />
      <span style={{ flex: 1 }}>
        <span style={{ fontWeight: 500 }}>{title}</span>
        <br />
        <span style={{ color: mut }}>{desc}</span>
      </span>
      </label>
      {mode === m && <div style={{ paddingLeft: 26 }}>{extra}</div>}
    </div>
  );

  const tabs: { id: Tab; title: string; sub: string }[] = [
    {
      id: 'columns',
      title: 'Columns',
      sub: `${included.length} of ${columns?.length ?? 0} included${customTypes ? ` · ${customTypes} custom type${customTypes > 1 ? 's' : ''}` : ''}`,
    },
    ...(isClickHouse
      ? [
          {
            id: 'keys' as Tab,
            title: 'Sorting & partitioning',
            sub: `${sortKeys.length ? `${sortKeys.length} sort key${sortKeys.length > 1 ? 's' : ''}` : 'Primary key order'}${partitionBy ? ' · partitioned' : ''}`,
          },
        ]
      : []),
    {
      id: 'engine',
      title: isClickHouse ? 'Engine & storage' : 'Snapshot',
      sub: isClickHouse
        ? (props.engineOptions.find((o) => o.value === props.engineValue)?.label ?? '')
        : row.partitionKey || 'Default partitioning',
    },
  ];

  return (
    <div
      style={{
        marginTop: 4,
        border: `1px solid ${line}`,
        borderRadius: 10,
        background: theme.colors.base.background.normal,
        width: '100%',
      }}
    >
      <div
        role='tablist'
        style={{ display: 'flex', gap: 4, borderBottom: `1px solid ${line}`, padding: '0 12px' }}
      >
        {tabs.map((t) => (
          <TabButton
            key={t.id}
            type='button'
            role='tab'
            aria-selected={tab === t.id}
            onClick={() => setTab(t.id)}
            style={{
              display: 'flex',
              flexDirection: 'column',
              alignItems: 'flex-start',
              gap: 2,
              border: 0,
              background: 'none',
              padding: '12px 14px 10px',
              marginBottom: -1,
              font: 'inherit',
              fontSize: 13.5,
              fontWeight: 500,
              cursor: 'pointer',
              color: tab === t.id ? theme.colors.base.text.highContrast : mut,
              borderBottom: `2px solid ${tab === t.id ? theme.colors.base.text.highContrast : 'transparent'}`,
            }}
          >
            {t.title}
            <span style={{ fontSize: 12, fontWeight: 400, color: mut }}>{t.sub}</span>
          </TabButton>
        ))}
      </div>

      <div role='tabpanel' style={{ padding: 20 }}>
        {tab === 'columns' && children}

        {tab === 'keys' && isClickHouse && (
          <>
          <ThemedToastContainer containerId='sorting_key_warning' />
          <div
            style={{
              display: 'flex',
              flexWrap: 'wrap',
              alignItems: 'flex-start',
              gap: 28,
            }}
          >
            <div style={{ flex: '1 1 340px', minWidth: 0 }}>
              <p style={h4}>Sorting key</p>
              <p style={hint}>
                ORDER BY of the ClickHouse table. Empty uses the primary key
                {pkNames.length ? ` (${pkNames.join(', ')})` : ''}. Key columns
                can&apos;t be excluded.
              </p>
              {keyList('ordering', sortKeys, 'Add column to sorting key')}

              <p style={{ ...h4, marginTop: 28 }}>Partitioning</p>
              <p style={hint}>How ClickHouse splits the table on disk.</p>
              <div style={{ display: 'flex', flexDirection: 'column', gap: 8, marginTop: 12 }}>
                {radio('none', 'None', 'One partition. Fine for most tables under a few million rows.')}
                {radio(
                  'columns',
                  'By columns',
                  'PARTITION BY the selected columns as they are.',
                  keyList('partitioning', partKeys, 'Add partition column')
                )}
                {radio(
                  'expr',
                  'By expression',
                  'Any ClickHouse expression, e.g. toYYYYMM(created_at).',
                  <TextField
                    variant='simple'
                    aria-label='Partition by expression'
                    disabled={row.editingDisabled}
                    style={{ marginTop: 10, fontSize: 13, fontFamily: 'monospace' }}
                    placeholder='toYYYYMM(created_at)'
                    value={row.partitionByExpr}
                    onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                      props.onPartitionByExpr(e.target.value)
                    }
                  />
                )}
              </div>
            </div>
            {columns && (
            <aside
              aria-label='Resulting table'
              style={{
                flex: '0 1 240px',
                border: `1px solid ${line}`,
                borderRadius: 8,
                background: theme.colors.base.background.subtle,
                padding: '14px 16px',
              }}
            >
              <p style={{ margin: '0 0 8px', fontSize: 12, fontWeight: 600, color: mut }}>
                Resulting table
              </p>
              <pre
                style={{
                  margin: 0,
                  fontSize: 12,
                  lineHeight: 1.7,
                  whiteSpace: 'pre-wrap',
                  wordBreak: 'break-word',
                }}
              >
                {ddl}
              </pre>
              <p style={{ ...hint, marginTop: 10 }}>
                Approximate. PeerDB adds its own engine arguments.
              </p>
            </aside>
            )}
          </div>
          </>
        )}

        {tab === 'engine' && (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 18, maxWidth: 520 }}>
            {isClickHouse && (
              <>
                <div>
                  <span style={label}>Engine</span>
                  <ReactSelect
                    aria-label='Engine'
                    isDisabled={row.editingDisabled}
                    styles={engineOptionStyles}
                    theme={selectTheme}
                    options={props.engineOptions}
                    value={
                      props.engineOptions.find((o) => o.value === props.engineValue) ??
                      props.engineOptions[0]
                    }
                    onChange={(opt) => opt && props.onEngine(opt.value)}
                  />
                  <p style={hint}>
                    ReplacingMergeTree keeps the latest version of each row and is the
                    default for CDC.
                  </p>
                </div>
                <Field
                  label='Sharding key'
                  hint='Only for distributed tables. Leave empty otherwise.'
                  placeholder='e.g. rand()'
                  value={row.shardingKey}
                  disabled={row.editingDisabled}
                  onChange={props.onShardingKey}
                />
                <Field
                  label='Storage policy'
                  hint='ClickHouse storage policy name. Empty uses the server default.'
                  placeholder='Server default'
                  value={row.policyName}
                  disabled={row.editingDisabled}
                  onChange={props.onPolicyName}
                />
              </>
            )}
            <Field
              label='Snapshot partition column'
              hint='Column used to split the initial load into parallel chunks. Empty uses the default (ctid on Postgres).'
              placeholder='e.g. created_at'
              value={row.partitionKey}
              disabled={row.editingDisabled}
              onChange={props.onPartitionKey}
            />
          </div>
        )}
      </div>
    </div>
  );
}

const TabButton = styled.button`
  &:focus:not(:focus-visible) {
    outline: none;
  }
`;

function Field(p: {
  label: string;
  hint: string;
  placeholder: string;
  value: string | undefined;
  disabled: boolean;
  onChange: (v: string) => void;
}) {
  const theme = useStyledTheme();
  return (
    <div>
      <span style={{ display: 'block', fontSize: 13, fontWeight: 500, marginBottom: 6 }}>
        {p.label}
      </span>
      <TextField
        variant='simple'
        style={{ fontSize: 13 }}
        aria-label={p.label}
        disabled={p.disabled}
        placeholder={p.placeholder}
        value={p.value ?? ''}
        onChange={(e: React.ChangeEvent<HTMLInputElement>) => p.onChange(e.target.value)}
      />
      <p style={{ fontSize: 12.5, color: theme.colors.base.text.lowContrast, margin: '4px 0 0' }}>
        {p.hint}
      </p>
    </div>
  );
}

function IconButton(p: {
  label: string;
  icon: IconProps['name'];
  disabled: boolean;
  onClick: () => void;
}) {
  return (
    <button
      type='button'
      aria-label={p.label}
      title={p.label}
      disabled={p.disabled}
      onClick={p.onClick}
      style={{
        display: 'inline-flex',
        width: 28,
        height: 28,
        alignItems: 'center',
        justifyContent: 'center',
        border: 0,
        borderRadius: 6,
        background: 'none',
        cursor: p.disabled ? 'default' : 'pointer',
        opacity: p.disabled ? 0.35 : 1,
        padding: 0,
      }}
    >
      <Icon name={p.icon} />
    </button>
  );
}

function PkBadge() {
  const theme = useStyledTheme();
  return (
    <span
      style={{
        marginLeft: 6,
        fontSize: 11,
        fontWeight: 600,
        color: theme.colors.accent.text.lowContrast,
        background: theme.colors.accent.surface.normal,
        borderRadius: 4,
        padding: '1px 5px',
      }}
    >
      PK
    </span>
  );
}
