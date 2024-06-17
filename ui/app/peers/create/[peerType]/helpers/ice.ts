import { PeerSetter } from '@/app/dto/PeersDTO';
import {
  CommonIcebergCatalog,
  IcebergCatalog,
  IcebergConfig,
  IcebergIOConfig,
  IcebergS3IoConfig,
  JdbcIcebergCatalog,
} from '@/grpc_generated/peers';
import { PeerSetting } from './common';

export const CommonConfigSettings: PeerSetting[] = [
  {
    label: 'Catalog name',
    stateHandler: (value, setter) =>
      setter((curr) => {
        const currentIcebergConfig = curr as IcebergConfig;
        const currentCatalogConfig =
          currentIcebergConfig.catalogConfig ?? blankCatalogConfig;
        const newCatalogConfig = {
          ...currentCatalogConfig,
          commonConfig: {
            ...(currentCatalogConfig.commonConfig ?? blankCommonConfig),
            name: value as string,
          },
        };
        return { ...curr, catalogConfig: newCatalogConfig };
      }),
    tips: 'Name for the Iceberg Catalog (should be the same as used by the querying engine)',
    helpfulLink:
      'https://iceberg.apache.org/docs/1.5.2/configuration/?h=catalog#catalog-properties',
  },
  {
    label: 'URI',
    stateHandler: (value, setter) => {
      setter((curr) => {
        const currentIcebergConfig = curr as IcebergConfig;
        const currentCatalogConfig =
          currentIcebergConfig.catalogConfig ?? blankCatalogConfig;
        const newCatalogConfig: IcebergCatalog = {
          ...currentCatalogConfig,
          commonConfig: {
            ...(currentCatalogConfig.commonConfig ?? blankCommonConfig),
            uri: (value as string) || '',
          },
        };
        return { ...curr, catalogConfig: newCatalogConfig };
      });
    },
    tips: 'URI of the catalog (eg thrift://hive-host:9083)',
  },
  {
    label: 'Warehouse location',
    stateHandler: (value, setter) => {
      setter((curr) => {
        const currentIcebergConfig = curr as IcebergConfig;
        const currentCatalogConfig =
          currentIcebergConfig.catalogConfig ?? blankCatalogConfig;
        const newCatalogConfig: IcebergCatalog = {
          ...currentCatalogConfig,
          commonConfig: {
            ...(currentCatalogConfig.commonConfig ?? blankCommonConfig),
            warehouseLocation: value as string,
          },
        };
        return { ...curr, catalogConfig: newCatalogConfig };
      });
    },
    tips: 'URI to the warehouse location (eg s3://mybucket/mypath/subpath)',
  },
  {
    label: 'Client pool size',
    stateHandler: (value, setter) => {
      setter((curr) => {
        const currentIcebergConfig = curr as IcebergConfig;
        const currentIcebergCatalog =
          currentIcebergConfig.catalogConfig ?? blankCatalogConfig;
        const newCatalogConfig: IcebergCatalog = {
          ...currentIcebergCatalog,
          commonConfig: {
            ...(currentIcebergCatalog.commonConfig ?? blankCommonConfig),
            clientPoolSize: parseInt(value as string),
          },
        };
        return { ...curr, catalogConfig: newCatalogConfig };
      });
    },
    tips: 'Number of clients to keep in the pool',
    type: 'number',
    optional: true,
  },
];

export const FileIoSettings: PeerSetting[] = [
  {
    label: 'Access Key ID',
    stateHandler: (value, setter) => {
      setter((curr) => {
        const currentIcebergConfig = curr as IcebergConfig;
        const currentIcebergCatalog =
          currentIcebergConfig.catalogConfig ?? blankCatalogConfig;
        const currentIoConfig = currentIcebergCatalog.ioConfig ?? blankIoConfig;
        const newFileIoConfig: IcebergCatalog = {
          ...currentIcebergCatalog,
          ioConfig: {
            ...currentIoConfig,
            s3: {
              ...(currentIoConfig.s3 ?? blankS3IcebergConfig),
              accessKeyId: value as string,
            },
          },
        };
        return { ...curr, catalogConfig: newFileIoConfig };
      });
    },
    tips: 'The AWS access key ID associated with your account.',
    helpfulLink:
      'https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html',
  },
  {
    label: 'Secret Access Key',
    stateHandler: (value, setter) => {
      setter((curr) => {
        const currentIcebergConfig = curr as IcebergConfig;
        const currentIcebergCatalog =
          currentIcebergConfig.catalogConfig ?? blankCatalogConfig;
        const currentIoConfig = currentIcebergCatalog.ioConfig ?? blankIoConfig;
        const newFileIoConfig: IcebergCatalog = {
          ...currentIcebergCatalog,
          ioConfig: {
            ...currentIoConfig,
            s3: {
              ...(currentIoConfig.s3 ?? blankS3IcebergConfig),
              secretAccessKey: value as string,
            },
          },
        };
        return { ...curr, catalogConfig: newFileIoConfig };
      });
    },
    tips: 'The AWS secret access key associated with your account.',
    helpfulLink:
      'https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html',
  },
  {
    label: 'Endpoint',
    stateHandler: (value, setter) => {
      setter((curr) => {
        const currentIcebergConfig = curr as IcebergConfig;
        const currentIcebergCatalog =
          currentIcebergConfig.catalogConfig ?? blankCatalogConfig;
        const currentIoConfig = currentIcebergCatalog.ioConfig ?? blankIoConfig;
        const newFileIoConfig: IcebergCatalog = {
          ...currentIcebergCatalog,
          ioConfig: {
            ...currentIoConfig,
            s3: {
              ...(currentIoConfig.s3 ?? blankS3IcebergConfig),
              endpoint: value as string,
            },
          },
        };
        return { ...curr, catalogConfig: newFileIoConfig };
      });
    },
    tips: 'The endpoint of your S3 bucket. This is optional.',
    optional: true,
  },
  {
    label: 'Path style access',
    stateHandler: (value, setter) => {
      setter((curr) => {
        const currentIcebergConfig = curr as IcebergConfig;
        const currentIcebergCatalog =
          currentIcebergConfig.catalogConfig ?? blankCatalogConfig;
        const currentIoConfig = currentIcebergCatalog.ioConfig ?? blankIoConfig;
        const newFileIoConfig: IcebergCatalog = {
          ...currentIcebergCatalog,
          ioConfig: {
            ...currentIoConfig,
            s3: {
              ...(currentIoConfig.s3 ?? blankS3IcebergConfig),
              pathStyleAccess: value as boolean,
            },
          },
        };
        return { ...curr, catalogConfig: newFileIoConfig };
      });
    },
    type: 'switch',
    tips: 'Set to true to use for services like MinIO. This is optional',
    optional: true,
  },
];

export const JdbcConfigSettings: PeerSetting[] = [
  {
    label: 'User',
    stateHandler: (value, setter) => {
      setter((curr) => {
        const currentIcebergConfig = curr as IcebergConfig;
        const currentIcebergCatalog =
          currentIcebergConfig.catalogConfig ?? blankCatalogConfig;
        const jdbcCatalog: IcebergCatalog = {
          ...currentIcebergCatalog,
          jdbc: {
            ...(currentIcebergCatalog.jdbc ?? blankJdbcConfig),
            user: value as string,
          },
        };
        return { ...curr, catalogConfig: jdbcCatalog };
      });
    },
    tips: 'Username for the JDBC connection',
    helpfulLink: 'https://iceberg.apache.org/docs/1.5.2/jdbc/',
  },
  {
    label: 'Password',
    stateHandler: (value, setter) => {
      setter((curr) => {
        const currentIcebergConfig = curr as IcebergConfig;
        const currentIcebergCatalog =
          currentIcebergConfig.catalogConfig ?? blankCatalogConfig;
        const jdbcCatalog: IcebergCatalog = {
          ...currentIcebergCatalog,
          jdbc: {
            ...(currentIcebergCatalog.jdbc ?? blankJdbcConfig),
            password: value as string,
          },
        };
        return { ...curr, catalogConfig: jdbcCatalog };
      });
    },
    tips: 'Password for the JDBC connection',
    type: 'password',
  },
  {
    label: 'Use SSL?',
    stateHandler: (value, setter) => {
      setter((curr) => {
        const currentIcebergConfig = curr as IcebergConfig;
        const currentIcebergCatalog =
          currentIcebergConfig.catalogConfig ?? blankCatalogConfig;
        const jdbcCatalog: IcebergCatalog = {
          ...currentIcebergCatalog,
          jdbc: {
            ...(currentIcebergCatalog.jdbc ?? blankJdbcConfig),
            useSsl: value as boolean,
          },
        };
        return { ...curr, catalogConfig: jdbcCatalog };
      });
    },
    type: 'switch',
    optional: true,
    tips: 'To enables SSL for the JDBC connection',
  },
  {
    label: 'Verify server certificate?',
    stateHandler: (value, setter) => {
      setter((curr) => {
        const currentIcebergConfig = curr as IcebergConfig;
        const currentIcebergCatalog =
          currentIcebergConfig.catalogConfig ?? blankCatalogConfig;
        const jdbcCatalog: IcebergCatalog = {
          ...currentIcebergCatalog,
          jdbc: {
            ...(currentIcebergCatalog.jdbc ?? blankJdbcConfig),
            verifyServerCertificate: value as boolean,
          },
        };
        return { ...curr, catalogConfig: jdbcCatalog };
      });
    },
    type: 'switch',
    optional: true,
    tips: 'To verify the server certificate for the JDBC connection. This is optional',
  },
];

export const handleHiveSelection = (setter: PeerSetter) => {
  setter((curr) => {
    const currentIcebergConfig = curr as IcebergConfig;
    const currentCatalogConfig =
      currentIcebergConfig.catalogConfig ?? blankCatalogConfig;
    const newCatalogConfig: IcebergCatalog = {
      ...currentCatalogConfig,
      jdbc: undefined,
      hive: {},
    };
    return { ...curr, catalogConfig: newCatalogConfig };
  });
};

const blankCommonConfig: CommonIcebergCatalog = {
  name: '',
  uri: '',
  warehouseLocation: '',
  clientPoolSize: undefined,
  hadoopProperties: {},
};

const blankS3IcebergConfig: IcebergS3IoConfig = {
  accessKeyId: '',
  secretAccessKey: '',
  endpoint: '',
  pathStyleAccess: false,
};

const blankIoConfig: IcebergIOConfig = {
  s3: {
    accessKeyId: '',
    secretAccessKey: '',
    endpoint: '',
    pathStyleAccess: false,
  },
};

const blankJdbcConfig: JdbcIcebergCatalog = {
  user: '',
  password: '',
  useSsl: false,
  verifyServerCertificate: false,
};

const blankCatalogConfig: IcebergCatalog = {
  commonConfig: blankCommonConfig,
  ioConfig: blankIoConfig,
  hive: undefined,
  hadoop: undefined,
  rest: undefined,
  glue: undefined,
  jdbc: undefined,
  nessie: undefined,
};

export const blankIcebergConfig: IcebergConfig = {
  catalogConfig: blankCatalogConfig,
};
