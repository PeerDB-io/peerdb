# syntax=docker/dockerfile:1.17@sha256:38387523653efa0039f8e1c89bb74a30504e76ee9f565e25c9a09841f9427b05

# Base stage
FROM node:24-alpine@sha256:6a61dbdf1dd6adbfcf0c09237f8f0f7a101b479b0491f2e49b114e55f027ba16 AS base
ENV TZ=UTC
ENV NPM_CONFIG_UPDATE_NOTIFIER=false
RUN apk add --no-cache openssl && \
  mkdir /app && \
  chown -R node:node /app
ENV NEXT_TELEMETRY_DISABLED=1
USER node
WORKDIR /app

# Dependencies stage
FROM base AS builder
COPY --chown=node:node ui/package.json ui/package-lock.json ./
RUN npm ci
COPY --chown=node:node ui/ .
RUN npm run build

# Builder stage
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
