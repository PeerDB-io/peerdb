# syntax=docker/dockerfile:1.2

# Base stage
FROM node:18-bookworm-slim AS base
WORKDIR /app

# Dependencies stage
FROM base AS deps
WORKDIR /app
# Copy package.json and yarn.lock from ui folder to current directory in image
COPY ui/package.json ./
RUN yarn install

# Builder stage
FROM base AS runner
WORKDIR /app
COPY --from=deps /app/node_modules ./node_modules
# Copy all files from ui folder to current directory in image
COPY ui/ ./
RUN yarn build

EXPOSE 3000
ENV PORT 3000
ENV HOSTNAME "0.0.0.0"

CMD ["yarn", "start"]
