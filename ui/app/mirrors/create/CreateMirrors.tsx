'use client';
import { useSelectTheme } from '@/app/styles/select';
import { notifyErr } from '@/app/utils/notify';
import QRepQueryTemplate from '@/app/utils/qreptemplate';
import InfoPopover from '@/components/InfoPopover';
import { DBTypeToImageMapping } from '@/components/PeerComponent';
import PeerDBCodeEditor from '@/components/PeerDBCodeEditor';
import { RequiredIndicator } from '@/components/RequiredIndicator';
import ThemedToastContainer from '@/components/ThemedToastContainer';
import { QRepConfig, TableMapping } from '@/grpc_generated/flow';
import { DBType, dBTypeToJSON } from '@/grpc_generated/peers';
import { ListPeersResponse, PeerListItem } from '@/grpc_generated/route';
import { Button } from '@/lib/Button';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { ProgressCircle } from '@/lib/ProgressCircle';
import { TextField } from '@/lib/TextField';
import Image from 'next/image';
import Link from 'next/link';
import { useRouter, useSearchParams } from 'next/navigation';
import { useTheme } from 'styled-components';
import React, {
  useCallback,
  useEffect,
  useState,
  useTransition,
} from 'react';
import ReactSelect, { SingleValue } from 'react-select';
import CreatableSelect from 'react-select/creatable';
import { CDCConfig, MirrorType, TableMapRow } from '../../dto/MirrorsDTO';
import CDCConfigForm from './cdc/cdc';
import TablePicker from './cdc/tablemapping';
import {
  handleCreateCDC,
  handleCreateQRep,
  handleValidateCDC,
} from './handlers';
import { cdcSettings } from './helpers/cdc';
import {
  blankCDCSetting,
  blankQRepSetting,
  cdcSourceDefaults,
} from './helpers/common';
import { qrepSettings } from './helpers/qrep';
import MirrorCards from './mirrorcards';
import QRepConfigForm from './qrep/qrep';
import {
  fetchClickHouseDatabases,
  useClickHouseDatabase,
} from './destinationDatabase';
import { flowNameSchema } from './schema';
import {
  Band,
  StatusDot,
  actionBarStyle,
  connectorKeyframes,
} from './bands';
import * as styles from './styles';

function getPeerValue(peer: PeerListItem) {
  return peer.name;
}

function getPeerLabel(peer: PeerListItem) {
  return (
    <div style={{ display: 'flex', alignItems: 'center' }}>
      <div style={{ width: '5%', height: '5%' }}>
        <Image
          src={DBTypeToImageMapping(peer.type)}
          alt='me'
          width={500}
          height={500}
          style={{ objectFit: 'cover' }}
        />
      </div>
      <div style={{ marginLeft: '1rem' }}>{peer.name}</div>
    </div>
  );
}

type CreateMirrorsProps = {
  peersPromise: Promise<ListPeersResponse>;
};

export default function CreateMirrors({ peersPromise }: CreateMirrorsProps) {
  const peers = React.use(peersPromise);
  const selectTheme = useSelectTheme();
  const router = useRouter();
  const mirrorParam = useSearchParams();
  const mirrorTypeParam = mirrorParam.get('type');
  const [mirrorName, setMirrorName] = useState<string>('');
  const [mirrorType, setMirrorType] = useState<MirrorType>(
    (mirrorTypeParam as MirrorType) ?? MirrorType.CDC
  );
  const [creating, startCreate] = useTransition();
  const [validating, startValidate] = useTransition();
  const [qrepConfig, setQrepConfig] = useState<QRepConfig>(blankQRepSetting);
  const [cdcConfig, setCdcConfig] = useState<CDCConfig>(blankCDCSetting);
  const [sourceType, setSourceType] = useState<DBType>(DBType.DBTYPE_UNKNOWN);
  const [destinationType, setDestinationType] = useState<DBType>(
    DBType.DBTYPE_UNKNOWN
  );
  const [rows, setRows] = useState<TableMapRow[]>([]);
  const [qrepQuery, setQrepQuery] = useState<string>(QRepQueryTemplate);
  const [nameValidityMessage, setNameValidityMessage] = useState<string>('');
  // ClickHouse destination: database to write to ('' keeps the peer's own).
  const [destinationDatabase, setDestinationDatabase] = useState<string>('');
  const [destinationDatabases, setDestinationDatabases] = useState<string[]>([]);
  const [peerDatabase, setPeerDatabase] = useState<string>('');
  const [databasesError, setDatabasesError] = useState<string>('');
  const destinationIsClickHouse =
    destinationType.toString() === DBType[DBType.CLICKHOUSE] ||
    dBTypeToJSON(destinationType) === 'CLICKHOUSE';

  const setSourcePeer = useCallback((peer: SingleValue<PeerListItem>) => {
    if (!peer) return;
    setQrepConfig((curr) => ({
      ...curr,
      sourceName: peer.name,
    }));
    setCdcConfig((curr) => ({
      ...curr,
      sourceName: peer.name,
    }));
    setSourceType(peer.type);

    const typeDefaults = cdcSourceDefaults[peer.type];
    if (typeDefaults) {
      setCdcConfig((curr) => ({
        ...curr,
        ...typeDefaults,
      }));
    }
  }, []);

  const setDestinationPeer = useCallback((peer: SingleValue<PeerListItem>) => {
    if (!peer) return;
    setQrepConfig((curr) => ({
      ...curr,
      destinationName: peer.name,
    }));
    setCdcConfig((curr) => ({
      ...curr,
      destinationName: peer.name,
    }));
    setDestinationType(peer.type);
    setDestinationDatabase('');
  }, []);

  useEffect(() => {
    const peerName = cdcConfig.destinationName;
    setDatabasesError('');
    if (!destinationIsClickHouse || !peerName) {
      setDestinationDatabases([]);
      setPeerDatabase('');
      return;
    }
    let cancelled = false;
    fetchClickHouseDatabases(peerName)
      .then(({ databases, currentDatabase }) => {
        if (cancelled) return;
        setDestinationDatabases(databases);
        setPeerDatabase(currentDatabase);
      })
      .catch((err: Error) => {
        if (cancelled) return;
        setDestinationDatabases([]);
        setPeerDatabase('');
        setDatabasesError(err.message);
      });
    return () => {
      cancelled = true;
    };
  }, [cdcConfig.destinationName, destinationIsClickHouse]);

  // Resolves the destination the mirror will use. Another database than the
  // peer's own becomes `<peer>__<database>`, which the backend resolves from the
  // peer without creating one. A new database is only created when the user
  // validates or creates the mirror, so browsing the list has no side effects.
  const resolveCdcConfig = async (): Promise<CDCConfig | undefined> => {
    if (
      !destinationIsClickHouse ||
      !destinationDatabase ||
      destinationDatabase === peerDatabase
    ) {
      return cdcConfig;
    }
    try {
      const peerName = await useClickHouseDatabase(
        cdcConfig.destinationName,
        destinationDatabase,
        !destinationDatabases.includes(destinationDatabase)
      );
      return { ...cdcConfig, destinationName: peerName };
    } catch (err) {
      notifyErr(
        `Could not use database ${destinationDatabase}: ${(err as Error).message}`
      );
      return undefined;
    }
  };

  const listMirrorsPage = () => {
    router.push('/mirrors');
  };

  const validateMirrorName = (e: React.ChangeEvent<HTMLInputElement>) => {
    const currentlyTypedMirrorName = e.target.value;
    if (currentlyTypedMirrorName !== '') {
      const flowNameValid = flowNameSchema.safeParse(currentlyTypedMirrorName);
      if (!flowNameValid.success) {
        setNameValidityMessage(flowNameValid.error.message);
      } else {
        setNameValidityMessage('');
      }
    }

    setMirrorName(currentlyTypedMirrorName);
  };


  const theme = useTheme();
  const selectedCount = rows.filter((row) => row.selected).length;
  // The peers list returns the type as a string, the state may hold either.
  const typeName = (type: DBType | string) => {
    const name = typeof type === 'string' ? type : dBTypeToJSON(type);
    const pretty: Record<string, string> = {
      POSTGRES: 'PostgreSQL',
      CLICKHOUSE: 'ClickHouse',
      MYSQL: 'MySQL',
      MONGO: 'MongoDB',
      BIGQUERY: 'BigQuery',
      SNOWFLAKE: 'Snowflake',
    };
    return name === 'DBTYPE_UNKNOWN' || name === 'UNRECOGNIZED'
      ? ''
      : (pretty[name] ?? name);
  };

  const peerPanel = (peerEnd: 'src' | 'dst') => {
    const isSource = peerEnd === 'src';
    const name = isSource ? cdcConfig.sourceName : cdcConfig.destinationName;
    const type = isSource ? sourceType : destinationType;
    return (
      <div
        style={{
          display: 'flex',
          flexDirection: 'column',
          rowGap: '0.9rem',
          padding: '1.25rem',
          borderRadius: '0.9rem',
          background: theme.colors.base.background.normal,
          border:
            !isSource && name
              ? `1px solid ${theme.colors.accent.fill.normal}`
              : `1px solid ${theme.colors.base.border.normal}`,
          boxShadow:
            !isSource && name
              ? `inset 0 0 0 1px ${theme.colors.accent.fill.normal}`
              : 'none',
        }}
      >
        <div
          style={{
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center',
          }}
        >
          <span
            style={{
              fontSize: 13,
              fontWeight: 600,
              color: theme.colors.base.text.lowContrast,
            }}
          >
            {isSource ? 'Source' : 'Destination'}
            {RequiredIndicator(true)}
          </span>
          <StatusDot
            ready={!!name}
            label={name ? 'Ready' : 'Select a peer'}
          />
        </div>
        <div
          style={{
            fontSize: 24,
            fontWeight: 500,
            letterSpacing: '-0.03em',
            lineHeight: 1.1,
            minHeight: '1.1em',
            overflowWrap: 'anywhere',
            color: name
              ? theme.colors.base.text.highContrast
              : theme.colors.base.fill.normal,
          }}
        >
          {name || 'No peer selected'}
        </div>
        {typeName(type) !== '' && (
          <div>
            <span
              style={{
                fontSize: 11.5,
                fontWeight: 600,
                padding: '2px 8px',
                borderRadius: 99,
                background: theme.colors.base.surface.normal,
                color: theme.colors.base.text.highContrast,
              }}
            >
              {typeName(type)}
            </span>
          </div>
        )}
        <div style={{ display: 'flex', alignItems: 'center' }}>
          <div style={{ width: '100%' }}>
            <ReactSelect
              instanceId={`peer-select-${peerEnd}`}
              aria-label={isSource ? 'Source peer' : 'Destination peer'}
              placeholder={`Select the ${isSource ? 'source' : 'destination'} peer`}
              onChange={isSource ? setSourcePeer : setDestinationPeer}
              options={
                (isSource ? peers.sourceItems : peers.destinationItems) ?? []
              }
              getOptionValue={getPeerValue}
              formatOptionLabel={getPeerLabel}
              theme={selectTheme}
            />
          </div>
          <InfoPopover
            tips={`The peer ${isSource ? 'from' : 'to'} which we will be replicating data. Ensure the prerequisites for this peer are met.`}
            link={
              'https://docs.peerdb.io/usecases/Real-time%20CDC/postgres-to-snowflake#prerequisites'
            }
          />
        </div>
        {!isSource && destinationIsClickHouse && name && (
          <div>
            <Label
              as='label'
              htmlFor='destination-database'
              style={{ fontWeight: 500, padding: 0 }}
            >
              Database
            </Label>
            <div style={{ marginTop: 6 }}>
              <CreatableSelect
                inputId='destination-database'
                aria-label='Destination database'
                theme={selectTheme}
                isDisabled={!!databasesError}
                placeholder={peerDatabase || 'Select or type a database'}
                value={
                  destinationDatabase || peerDatabase
                    ? {
                        value: destinationDatabase || peerDatabase,
                        label: destinationDatabase || peerDatabase,
                      }
                    : null
                }
                options={destinationDatabases.map((database) => ({
                  value: database,
                  label:
                    database === peerDatabase
                      ? `${database} (peer default)`
                      : database,
                }))}
                formatCreateLabel={(input) => `Create database "${input}"`}
                isValidNewOption={(input) =>
                  /^[a-z_][a-z0-9_]*$/.test(input) &&
                  !destinationDatabases.includes(input)
                }
                onChange={(option) =>
                  setDestinationDatabase(option?.value ?? '')
                }
              />
            </div>
            <div
              style={{
                marginTop: 6,
                fontSize: 12.5,
                lineHeight: 1.45,
                color: databasesError
                  ? theme.colors.destructive.text.lowContrast
                  : theme.colors.base.text.lowContrast,
              }}
            >
              {databasesError
                ? `Could not list databases: ${databasesError}`
                : destinationDatabase && destinationDatabase !== peerDatabase
                  ? destinationDatabases.includes(destinationDatabase)
                    ? `Tables are created in ${destinationDatabase}, using this peer's connection.`
                    : `${destinationDatabase} is created when you validate or create the mirror.`
                  : 'Tables are created in this database.'}
            </div>
          </div>
        )}
      </div>
    );
  };

  return (
    <div
      style={{
        width: '100%',
        maxWidth: 1120,
        margin: '0 auto',
        padding: '1rem 1.5rem 0',
      }}
    >
      <style>{connectorKeyframes}</style>
      <header style={{ paddingBottom: '1.5rem' }}>
        <nav
          aria-label='Breadcrumb'
          style={{ fontSize: 13, color: theme.colors.base.text.lowContrast }}
        >
          <Link href='/mirrors' style={{ color: 'inherit' }}>
            Mirrors
          </Link>{' '}
          / New
        </nav>
        <h1
          style={{
            margin: '0.5rem 0 0',
            fontSize: 36,
            lineHeight: 1.05,
            fontWeight: 600,
            letterSpacing: '-0.035em',
            color: theme.colors.base.text.highContrast,
          }}
        >
          Create mirror
        </h1>
        <p
          style={{
            margin: '0.6rem 0 0',
            fontSize: 15,
            color: theme.colors.base.text.lowContrast,
          }}
        >
          Replicate changes from a source database to a destination, in real
          time.
        </p>
      </header>

      <Band
        first
        title='Mirror type'
        description='CDC is the recommended and actively maintained type.'
      >
        <MirrorCards mirrorType={mirrorType} setMirrorType={setMirrorType} />
      </Band>

      <Band
        title='Name'
        description='Identifies the mirror in the list and in the logs.'
      >
        <div style={{ maxWidth: 380 }}>
          <Label as='label' htmlFor='mirror-name' style={{ fontWeight: 500 }}>
            Mirror Name
          </Label>
          <div style={{ marginTop: 6 }}>
            <TextField
              id='mirror-name'
              variant='simple'
              value={mirrorName}
              onChange={validateMirrorName}
            />
          </div>
          {nameValidityMessage && (
            <Label
              variant='footnote'
              colorName='lowContrast'
              colorSet='destructive'
            >
              {nameValidityMessage}
            </Label>
          )}
        </div>
      </Band>

      <Band
        title='Source and destination'
        description='Where the changes come from and where they are written.'
      >
        <div
          style={{
            display: 'grid',
            gridTemplateColumns: 'minmax(0, 1fr) 112px minmax(0, 1fr)',
            alignItems: 'stretch',
          }}
        >
          {peerPanel('src')}
          <div
            aria-hidden='true'
            style={{
              display: 'flex',
              flexDirection: 'column',
              alignItems: 'center',
              justifyContent: 'center',
              rowGap: 8,
              color: theme.colors.accent.text.lowContrast,
            }}
          >
            <span style={{ fontSize: 12, fontWeight: 500, textAlign: 'center' }}>
              {mirrorType === MirrorType.CDC ? 'CDC' : mirrorType}
            </span>
            <svg width='96' height='14' viewBox='0 0 96 14' fill='none'>
              <path
                className='pdb-flow'
                d='M4 7h80'
                stroke={theme.colors.accent.fill.normal}
                strokeWidth='2.5'
                strokeLinecap='round'
              />
              <path
                d='M82 2l9 5-9 5'
                stroke={theme.colors.accent.fill.normal}
                strokeWidth='2'
                strokeLinecap='round'
                strokeLinejoin='round'
              />
            </svg>
          </div>
          {peerPanel('dst')}
        </div>
      </Band>

      {mirrorType === MirrorType.QRep && (
        <Band
          title='Replication query'
          description='The results of this query are replicated to a target table.'
        >
          <div
            style={{
              display: 'flex',
              flexDirection: 'column',
              rowGap: '1rem',
            }}
          >
            <Label>
              For append and upsert modes, make sure the query{' '}
              <b>includes the start and end placeholders</b>. PeerDB uses these
              placeholders for partitioning query results for performance. In
              most cases, you will require a watermark table and a watermark
              column in that table.{' '}
              <Link
                style={{
                  color: theme.colors.accent.text.lowContrast,
                  cursor: 'pointer',
                  width: 'fit-content',
                }}
                target='_blank'
                href={
                  'https://docs.peerdb.io/usecases/Streaming%20Query%20Replication/postgres-to-snowflake#step-2-set-up-mirror-to-transform-and-sync-data'
                }
              >
                What does that mean?
              </Link>
            </Label>
            <PeerDBCodeEditor code={qrepQuery} setter={setQrepQuery} />
          </div>
        </Band>
      )}

      {mirrorType && (
        <Band
          title='Configuration'
          description='The defaults are enough to start. You can change them later.'
        >
          {!creating && <ThemedToastContainer />}
          {mirrorType === MirrorType.CDC ? (
            <CDCConfigForm
              settings={cdcSettings}
              mirrorConfig={cdcConfig}
              sourceType={sourceType}
              destinationType={destinationType}
              setter={setCdcConfig}
              rows={rows}
              setRows={setRows}
              hideTablePicker
            />
          ) : (
            <QRepConfigForm
              settings={qrepSettings}
              mirrorConfig={qrepConfig}
              sourceType={sourceType}
              destinationType={destinationType}
              setter={setQrepConfig}
              xmin={mirrorType === MirrorType.XMin}
            />
          )}
        </Band>
      )}

      {mirrorType === MirrorType.CDC &&
        cdcConfig.sourceName &&
        cdcConfig.destinationName && (
          <section
            style={{
              padding: '1.75rem 0 0',
              borderTop: `1px solid ${theme.colors.base.border.subtle}`,
            }}
          >
            <TablePicker
              redesign
              sourcePeerName={cdcConfig.sourceName}
              sourceType={sourceType}
              rows={rows}
              setRows={setRows}
              peerType={destinationType}
              alreadySelectedTablesMapping={new Map<string, TableMapping[]>()}
              initialLoadOnly={cdcConfig.initialSnapshotOnly}
            />
          </section>
        )}

      {mirrorType && (
        <div style={actionBarStyle(theme)}>
          <StatusDot
            ready={!!cdcConfig.sourceName}
            label={cdcConfig.sourceName ? 'Source ready' : 'No source'}
          />
          <StatusDot
            ready={!!cdcConfig.destinationName}
            label={
              cdcConfig.destinationName ? 'Destination ready' : 'No destination'
            }
          />
          {mirrorType === MirrorType.CDC && (
            <span
              style={{
                fontSize: 13,
                color: theme.colors.base.text.lowContrast,
              }}
            >
              {selectedCount} {selectedCount === 1 ? 'table' : 'tables'}{' '}
              selected
            </span>
          )}
          <span style={{ flexGrow: 1 }} />
          {mirrorType === MirrorType.CDC && (
            <Button
              style={styles.CreateButtonStyle}
              variant='peer'
              disabled={creating}
              onClick={() =>
                startValidate(async () => {
                  const config = await resolveCdcConfig();
                  if (!config) return;
                  await handleValidateCDC(
                    mirrorName,
                    rows,
                    config,
                    destinationType
                  );
                })
              }
            >
              {validating ? (
                <ProgressCircle variant='determinate_progress_circle' />
              ) : (
                <>
                  <Icon name='checklist' /> Validate
                </>
              )}
            </Button>
          )}
          <Button
            style={styles.CreateButtonStyle}
            variant='normalSolid'
            disabled={creating}
            onClick={() =>
              startCreate(async () => {
                if (mirrorType === MirrorType.CDC) {
                  const config = await resolveCdcConfig();
                  if (!config) return;
                  return handleCreateCDC(
                    mirrorName,
                    rows,
                    config,
                    destinationType,
                    listMirrorsPage
                  );
                }
                return handleCreateQRep(
                  mirrorName,
                  qrepQuery,
                  qrepConfig,
                  destinationType,
                  listMirrorsPage,
                  mirrorType === MirrorType.XMin
                );
              })
            }
          >
            {creating ? (
              <ProgressCircle variant='determinate_progress_circle' />
            ) : (
              <>
                <Icon name='add' /> Create Mirror
              </>
            )}
          </Button>
        </div>
      )}
    </div>
  );
}
