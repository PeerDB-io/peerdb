import next from 'eslint-config-next';
import prettier from 'eslint-config-prettier';

const config = [
  {
    ignores: [
      'node_modules',
      '.next',
      'dist',
      'build',
      '**/*.config.js',
      'grpc_generated/**',
    ],
  },
  ...next,
  prettier,
];

export default config;
