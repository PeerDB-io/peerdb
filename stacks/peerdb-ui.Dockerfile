# syntax=docker/dockerfile:1.19@sha256:b6afd42430b15f2d2a4c5a02b919e98a525b785b1aaff16747d2f623364e39b6

# Base stage
FROM node:24-alpine@sha256:f36fed0b2129a8492535e2853c64fbdbd2d29dc1219ee3217023ca48aebd3787 AS base
ENV TZ=UTC
ENV NPM_CONFIG_UPDATE_NOTIFIER=false
RUN apk add --no-cache openssl && \
  mkdir /app && \
  chown -R node:node /app
ENV NEXT_TELEMETRY_DISABLED=1
USER node
WORKDIR /app

# Dependencies stage
FROM base AS dependencies
COPY --chown=node:node ui/package.json ui/package-lock.json ./
# BuildKit cache mount for npm cache
RUN --mount=type=cache,target=/home/node/.npm,uid=1000,gid=1000 \
    npm ci

# Builder stage for production
FROM dependencies AS builder
COPY --chown=node:node ui/ .
RUN npm run build

# Dev stage
FROM dependencies AS dev
COPY --chown=node:node ui/ .
EXPOSE 3000
ENV PORT=3000
ENV HOSTNAME=0.0.0.0
ENV NODE_ENV=development
ARG PEERDB_VERSION_SHA_SHORT
ENV PEERDB_VERSION_SHA_SHORT=${PEERDB_VERSION_SHA_SHORT}
CMD ["npm", "run", "dev"]

# Runner stage for production
FROM base AS runner
ENV NODE_ENV=production

COPY --from=builder /app/public ./public

# Automatically leverage output traces to reduce image size
# https://nextjs.org/docs/advanced-features/output-file-tracing
COPY --from=builder /app/.next/standalone .
COPY --from=builder /app/.next/static ./.next/static
COPY --chown=node:node stacks/ui/ui-entrypoint.sh /app/entrypoint.sh

EXPOSE 3000

ENV PORT=3000
# set hostname to localhost
ENV HOSTNAME=0.0.0.0

ARG PEERDB_VERSION_SHA_SHORT
ENV PEERDB_VERSION_SHA_SHORT=${PEERDB_VERSION_SHA_SHORT}

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["node", "server.js"]

