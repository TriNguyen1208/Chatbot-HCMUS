# ==========================================
# Stage 1: Base (Môi trường chung)
# ==========================================
FROM node:22-alpine AS base
WORKDIR /app
# Cài đặt ffmpeg cho xử lý video đa phương tiện
RUN apk add --no-cache ffmpeg

# ==========================================
# Stage 2: Development (Dành cho Local Dev & Hot Reload)
# ==========================================
FROM base AS development
ENV NODE_ENV=development
COPY package*.json ./
RUN npm install
# Source code sẽ được mount từ docker-compose volume
EXPOSE 5000
CMD ["npm", "run", "dev:server"]

# ==========================================
# Stage 3: Builder (Biên dịch TypeScript)
# ==========================================
FROM base AS builder
COPY package*.json ./
RUN npm install
COPY . .
RUN npm run build

# ==========================================
# Stage 4: Production (Chạy sản phẩm thực tế)
# ==========================================
FROM base AS production
ENV NODE_ENV=production
ENV NODE_OPTIONS="--conditions=production"
COPY package*.json ./
# Chỉ cài production dependencies khi còn ở quyền root
RUN npm install --omit=dev && npm cache clean --force
# Copy source đã build từ stage builder
COPY --from=builder /app/dist ./dist
# Phân quyền cho user node và chuyển sang non-root user
RUN chown -R node:node /app
USER node

EXPOSE 5000
CMD ["node", "dist/index.js"]
