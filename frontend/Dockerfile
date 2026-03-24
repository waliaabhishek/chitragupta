# syntax=docker/dockerfile:1.6

# Stage 1: Builder — install deps and build static assets
FROM node:22-alpine AS builder

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci

COPY . .
RUN npm run build

# Stage 2: Runtime — nginx serving static files + API proxy
FROM nginx:1.27-alpine

# Remove default nginx config
RUN rm /etc/nginx/conf.d/default.conf

COPY --from=builder /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/chitragupta.conf

EXPOSE 80

# nginx handles SIGTERM natively — no custom entrypoint needed
CMD ["nginx", "-g", "daemon off;"]
