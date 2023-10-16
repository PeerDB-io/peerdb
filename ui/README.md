PeerDB Cloud Template

## Prerequisites

- [NodeJS](https://nodejs.org/en): `18.17.1`
- [yarn](https://yarnpkg.com/) package manager: `3.6.1`

## Getting Started

Install dependencies using [yarn](https://classic.yarnpkg.com/en/)

```bash
yarn
```

### Start the example project

Start the NextJS dev server that runs the example project.

```bash
yarn dev
```

Open [http://localhost:3000](http://localhost:3000) with your browser to see the result.

The example project files resides inside the `app` folder.

### Start the Storybook

Every UI component has a [Storybook](https://storybook.js.org/) story attached to it. To run and view the Storybook for the project run the following

```bash
yarn storybook
```

Open [http://localhost:6000](http://localhost:6000) with your browser to see the result.

The stories and their corresponding components resides inside the `lib` folder.

## Storybook Github pages

The Storybook in this repositories Github pages at [Storybook](https://peerdb-io.github.io/peerdb-cloud-template).

To deploy a new version of Storybook to Github pages run the script

```bash
yarn storybook:deploy
```

It will automatically run the storybook build, push the content to the branch `gh-pages` to automatically deply the newly built Storybook to Github pages.
