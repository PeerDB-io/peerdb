# syntax=docker/dockerfile:1.2

# Base stage
FROM node:18-bookworm-slim AS base
RUN apt-get update && apt-get install -y openssl
WORKDIR /app

# Dependencies stage
FROM base AS deps
WORKDIR /app
# Copy package.json and yarn.lock from ui folder to current directory in image
COPY ui/package.json ui/yarn.lock* ui/.yarnrc.yml ./
RUN yarn --frozen-lockfile

FROM base AS builder
WORKDIR /app

COPY --from=deps /app/node_modules ./node_modules
COPY ui/ ./

# Prisma
RUN yarn prisma generate

ENV NEXT_TELEMETRY_DISABLED 1
RUN yarn build

# Builder stage
FROM base AS runner
WORKDIR /app
ENV NODE_ENV production
ENV NEXT_TELEMETRY_DISABLED 1
RUN addgroup --system --gid 1001 nodejs
RUN adduser --system --uid 1001 nextjs

COPY --from=builder /app/public ./public

# Set the correct permission for prerender cache
RUN mkdir .next
RUN chown nextjs:nodejs .next

# Automatically leverage output traces to reduce image size
# https://nextjs.org/docs/advanced-features/output-file-tracing
COPY --from=builder --chown=nextjs:nodejs /app/.next/standalone ./
COPY --from=builder --chown=nextjs:nodejs /app/.next/static ./.next/static
COPY stacks/ui/ui-entrypoint.sh /app/entrypoint.sh

# allow permissions for nextjs user to do anything in /app
RUN chown -R nextjs:nodejs /app

USER nextjs

EXPOSE 3000

ENV PORT 3000
# set hostname to localhost
ENV HOSTNAME "0.0.0.0"

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["node", "server.js"]
