# syntax=docker/dockerfile:1.2

# Base stage
FROM node:20-bookworm-slim AS base
ENV NPM_CONFIG_UPDATE_NOTIFIER=false
RUN apt-get update && \
  apt-get install -y openssl && \
  mkdir /app && \
  chown -R node:node /app
USER node
WORKDIR /app

# Dependencies stage
FROM base AS builder
COPY --chown=node:node ui/package.json ui/package-lock.json .
RUN npm ci
COPY --chown=node:node ui/ .

# Prisma
RUN npx prisma generate

ENV NEXT_TELEMETRY_DISABLED 1
RUN npm run build

# Builder stage
FROM base AS runner
ENV NODE_ENV production
ENV NEXT_TELEMETRY_DISABLED 1

COPY --from=builder /app/public ./public

# Automatically leverage output traces to reduce image size
# https://nextjs.org/docs/advanced-features/output-file-tracing
COPY --from=builder /app/.next/standalone .
COPY --from=builder /app/.next/static ./.next/static
COPY --chown=node:node stacks/ui/ui-entrypoint.sh /app/entrypoint.sh

EXPOSE 3000

ENV PORT 3000
# set hostname to localhost
ENV HOSTNAME "0.0.0.0"

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["node", "server.js"]
