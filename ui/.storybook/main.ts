import type { StorybookConfig } from '@storybook/nextjs';
import CopyPlugin from 'copy-webpack-plugin';
import path from 'path';

const MATERIAL_ICONS_FONT_NODE_MODULE_PATH =
  'node_modules/material-symbols/material-symbols-sharp.woff2';

const config: StorybookConfig = {
  stories: ['../lib/**/*.mdx', '../lib/**/*.stories.@(js|jsx|mjs|ts|tsx)'],
  addons: [
    '@storybook/addon-links',
    '@storybook/addon-essentials',
    '@storybook/addon-interactions',
    {
      name: '@storybook/addon-styling',
      options: {
        postCss: {
          implementation: require.resolve('postcss'),
        },
      },
    },
  ],
  framework: {
    name: '@storybook/nextjs',
    options: {},
  },
  docs: {
    autodocs: 'tag',
  },
  staticDirs: ['./public'],
  webpackFinal(config) {
    const iconsPath = path.resolve(
      __dirname,
      '..',
      MATERIAL_ICONS_FONT_NODE_MODULE_PATH
    );

    const toPath = path.resolve(
      __dirname,
      'public',
      MATERIAL_ICONS_FONT_NODE_MODULE_PATH
    );

    config.plugins?.push(
      new CopyPlugin({
        patterns: [
          {
            from: iconsPath,
            to: toPath,
          },
        ],
      })
    );
    return config;
  },
};
export default config;
