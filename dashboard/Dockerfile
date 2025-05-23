FROM node:18-alpine AS base

# Build the dashboard directoy, only what is needed.
FROM base AS deps
# Check https://github.com/nodejs/docker-node/tree/b4117f9333da4138b03a546ec926ef50a31506c3#nodealpine to understand why libc6-compat might be needed.
RUN apk add --no-cache libc6-compat
WORKDIR /app/dashboard

COPY /dashboard/package.json /dashboard/.yarn.lock* /dashboard/package-lock.json* ./
RUN npm i

# Rebuild the source code only when needed
FROM base AS builder
WORKDIR /app
COPY --from=deps /app/dashboard/node_modules ./dashboard/node_modules
COPY ./dashboard ./dashboard
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

EXPOSE 80
ENV PORT 80

CMD ["node", "server.js"]

COPY ./nginx.conf /etc/nginx/nginx.conf
