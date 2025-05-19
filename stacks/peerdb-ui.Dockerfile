# syntax=docker/dockerfile:1.15@sha256:9857836c9ee4268391bb5b09f9f157f3c91bb15821bb77969642813b0d00518d

# Base stage
FROM node:22-alpine@sha256:152270cd4bd094d216a84cbc3c5eb1791afb05af00b811e2f0f04bdc6c473602 AS base
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
