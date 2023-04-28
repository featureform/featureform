FROM node:16-alpine AS base

# Build the dashboard directoy, only what is needed.
FROM base AS deps
# Check https://github.com/nodejs/docker-node/tree/b4117f9333da4138b03a546ec926ef50a31506c3#nodealpine to understand why libc6-compat might be needed.
RUN apk add --no-cache libc6-compat
WORKDIR /app/dashboard

# Install dependencies based on the preferred package manager
COPY package.json yarn.lock* package-lock.json* pnpm-lock.yaml* ./
RUN \
  if [ -f yarn.lock ]; then yarn --frozen-lockfile; \
  elif [ -f package-lock.json ]; then npm ci; \
  else echo "Lockfile not found." && exit 1; \
  fi

# Rebuild the source code only when needed
FROM base AS builder
WORKDIR /app
COPY --from=deps /app/dashboard/node_modules ./dashboard/node_modules
COPY . ./dashboard
ENV NEXT_TELEMETRY_DISABLED 1

WORKDIR /app/dashboard
RUN npm run build

#Production image, copy all the files and run next
FROM base AS runner
WORKDIR /app/dashboard
ENV NODE_ENV production
ENV NEXT_TELEMETRY_DISABLED 1
RUN addgroup --system --gid 1001 nodejs
RUN adduser --system --uid 1001 nextjs
COPY --from=builder /app/dashboard/public ./public

COPY --from=builder --chown=nextjs:nodejs /app/dashboard/.next/standalone ./
COPY --from=builder --chown=nextjs:nodejs /app/dashboard/.next/static ./.next/static
COPY --from=builder --chown=nextjs:nodejs /app/dashboard/out ./out

#todox: swap to port 80. but edit the nginx config first.
EXPOSE 3007
ENV PORT 3007

CMD ["node", "server.js"]

COPY ./nginx.conf /etc/nginx/nginx.conf
