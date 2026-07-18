# Multi-stage Dockerfile for GaungNusa (Next.js + Python ETL/ML Worker)
FROM node:20-slim AS base

# Install runtime Python 3 and curl (lightweight, uses pre-built wheels)
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies for ETL & ML Worker
COPY requirements.txt ./
RUN pip3 install --no-cache-dir -r requirements.txt --break-system-packages

# Install Node.js dependencies
COPY package.json package-lock.json ./
RUN npm ci

# Copy full application code
COPY . .

# Generate Prisma Client
RUN npx prisma@5 generate

# Build Next.js application
ENV NEXT_TELEMETRY_DISABLED=1
ENV NODE_ENV=production
RUN npm run build

# Expose port
EXPOSE 3000

ENV PORT=3000
ENV HOSTNAME="0.0.0.0"

# Entrypoint script: Sync Prisma schema then launch Next.js server
CMD ["sh", "-c", "npx prisma db push && npm start"]
