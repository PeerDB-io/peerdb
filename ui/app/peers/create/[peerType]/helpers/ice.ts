import { IcebergCatalog, IcebergConfig } from '@/grpc_generated/peers';
import { PeerSetting } from './common';

export const CommonConfigSettings: PeerSetting[] = [
  {
    label: 'Catalog Name',
    stateHandler: (value, setter) =>
      setter((curr) => {
        const currentIcebergConfig = curr as IcebergConfig;
        const newCatalogConfig = {
          ...currentIcebergConfig.catalogConfig!,
          commonConfig: {
            ...currentIcebergConfig.catalogConfig!.commonConfig!,
            name: value as string,
          },
        };
        return { ...curr, catalogConfig: newCatalogConfig };
      }),
  },
  {
    label: 'URI',
    stateHandler: (value, setter) => {
      setter((curr) => {
        const currentIcebergConfig = curr as IcebergConfig;
        const newCatalogConfig: IcebergCatalog = {
          ...currentIcebergConfig.catalogConfig!,
          commonConfig: {
            ...currentIcebergConfig.catalogConfig!.commonConfig!,
            uri: value as string,
          },
        };
        return { ...curr, catalogConfig: newCatalogConfig };
      });
    },
  },
  {
    label: 'Warehouse location',
    stateHandler: (value, setter) => {
      setter((curr) => {
        const currentIcebergConfig = curr as IcebergConfig;
        const newCatalogConfig: IcebergCatalog = {
          ...currentIcebergConfig.catalogConfig!,
          commonConfig: {
            ...currentIcebergConfig.catalogConfig!.commonConfig!,
            warehouseLocation: value as string,
          },
        };
        return { ...curr, catalogConfig: newCatalogConfig };
      });
    },
  },
  {
    label: 'Client pool size',
    stateHandler: (value, setter) => {
      setter((curr) => {
        const currentIcebergConfig = curr as IcebergConfig;
        const newCatalogConfig: IcebergCatalog = {
          ...currentIcebergConfig.catalogConfig!,
          commonConfig: {
            ...currentIcebergConfig.catalogConfig!.commonConfig!,
            clientPoolSize: parseInt(value as string),
          },
        };
        return { ...curr, catalogConfig: newCatalogConfig };
      });
    },
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
        const newFileIoConfig: IcebergCatalog = {
          ...currentIcebergConfig.catalogConfig!,
          ioConfig: {
            ...currentIcebergConfig.catalogConfig!.ioConfig!,
            s3: {
              ...currentIcebergConfig.catalogConfig!.ioConfig!.s3!,
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
        const newFileIoConfig: IcebergCatalog = {
          ...currentIcebergConfig.catalogConfig!,
          ioConfig: {
            ...currentIcebergConfig.catalogConfig!.ioConfig!,
            s3: {
              ...currentIcebergConfig.catalogConfig!.ioConfig!.s3!,
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
        const newFileIoConfig: IcebergCatalog = {
          ...currentIcebergConfig.catalogConfig!,
          ioConfig: {
            ...currentIcebergConfig.catalogConfig!.ioConfig!,
            s3: {
              ...currentIcebergConfig.catalogConfig!.ioConfig!.s3!,
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
        const newFileIoConfig: IcebergCatalog = {
          ...currentIcebergConfig.catalogConfig!,
          ioConfig: {
            ...currentIcebergConfig.catalogConfig!.ioConfig!,
            s3: {
              ...currentIcebergConfig.catalogConfig!.ioConfig!.s3!,
              pathStyleAccess: value as string,
            },
          },
        };
        return { ...curr, catalogConfig: newFileIoConfig };
      });
    },
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
        const jdbcCatalog: IcebergCatalog = {
          ...currentIcebergConfig.catalogConfig!,
          jdbc: {
            ...currentIcebergConfig.catalogConfig!.jdbc!,
            user: value as string,
          },
        };
        return { ...curr, catalogConfig: jdbcCatalog };
      });
    },
  },
  {
    label: 'Password',
    stateHandler: (value, setter) => {
      setter((curr) => {
        const currentIcebergConfig = curr as IcebergConfig;
        const jdbcCatalog: IcebergCatalog = {
          ...currentIcebergConfig.catalogConfig!,
          jdbc: {
            ...currentIcebergConfig.catalogConfig!.jdbc!,
            password: value as string,
          },
        };
        return { ...curr, catalogConfig: jdbcCatalog };
      });
    },
    type: 'password',
  },
  {
    label: 'Use SSL?',
    stateHandler: (value, setter) => {
      setter((curr) => {
        const currentIcebergConfig = curr as IcebergConfig;
        const jdbcCatalog: IcebergCatalog = {
          ...currentIcebergConfig.catalogConfig!,
          jdbc: {
            ...currentIcebergConfig.catalogConfig!.jdbc!,
            useSsl: value as boolean,
          },
        };
        return { ...curr, catalogConfig: jdbcCatalog };
      });
    },
    type: 'switch',
    optional: true,
  },
  {
    label: 'Verify server certificate?',
    stateHandler: (value, setter) => {
      setter((curr) => {
        const currentIcebergConfig = curr as IcebergConfig;
        const jdbcCatalog: IcebergCatalog = {
          ...currentIcebergConfig.catalogConfig!,
          jdbc: {
            ...currentIcebergConfig.catalogConfig!.jdbc!,
            verifyServerCertificate: value as boolean,
          },
        };
        return { ...curr, catalogConfig: jdbcCatalog };
      });
    },
    type: 'switch',
    optional: true,
  },
];

export const blankIcebergConfig: IcebergConfig = {
  catalogConfig: undefined,
};
