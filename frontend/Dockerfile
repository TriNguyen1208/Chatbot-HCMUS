# ==========================================
# Stage 1: Base (Môi trường chung)
# ==========================================
FROM node:22-alpine AS base
WORKDIR /app
RUN apk add --no-cache libc6-compat

# ==========================================
# Stage 2: Development (Dành cho Dev & Hot Module Reload)
# ==========================================
FROM base AS development
ENV NODE_ENV=development
COPY package*.json ./
RUN npm install --legacy-peer-deps
EXPOSE 3000
CMD ["npm", "run", "dev"]

# ==========================================
# Stage 3: Dependencies cho Build
# ==========================================
FROM base AS deps
COPY package*.json ./
RUN npm ci --legacy-peer-deps

# ==========================================
# Stage 4: Builder (Biên dịch Next.js)
# ==========================================
FROM base AS builder
COPY --from=deps /app/node_modules ./node_modules
COPY . .

# Biến môi trường public cần thiết lúc build
ARG NEXT_PUBLIC_API_URL
ENV NEXT_PUBLIC_API_URL=$NEXT_PUBLIC_API_URL

ARG NEXT_PUBLIC_BASE_URL
ENV NEXT_PUBLIC_BASE_URL=$NEXT_PUBLIC_BASE_URL

ARG NEXT_PUBLIC_GOOGLE_CLIENT_ID
ENV NEXT_PUBLIC_GOOGLE_CLIENT_ID=$NEXT_PUBLIC_GOOGLE_CLIENT_ID

ARG NEXT_PUBLIC_MICROSOFT_CLIENT_ID
ENV NEXT_PUBLIC_MICROSOFT_CLIENT_ID=$NEXT_PUBLIC_MICROSOFT_CLIENT_ID

ARG NEXT_PUBLIC_MICROSOFT_TENANT_ID
ENV NEXT_PUBLIC_MICROSOFT_TENANT_ID=$NEXT_PUBLIC_MICROSOFT_TENANT_ID

ENV NEXT_TELEMETRY_DISABLED=1
RUN npm run build

# ==========================================
# Stage 5: Production (Chạy Standalone Runner siêu nhẹ)
# ==========================================
FROM base AS production
ENV NODE_ENV=production
ENV NEXT_TELEMETRY_DISABLED=1

RUN addgroup --system --gid 1001 nodejs && \
    adduser --system --uid 1001 nextjs

# Copy static assets và standalone build từ stage builder
COPY --from=builder /app/public ./public
COPY --from=builder --chown=nextjs:nodejs /app/.next/standalone ./
COPY --from=builder --chown=nextjs:nodejs /app/.next/static ./.next/static

USER nextjs

EXPOSE 3000
ENV PORT=3000
ENV HOSTNAME="0.0.0.0"

CMD ["node", "server.js"]
