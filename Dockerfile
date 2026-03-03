FROM node:20-slim@sha256:8cb5dbe5f78f5b6b6e5c5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5

WORKDIR /app

RUN npm ci --only=production

COPY package*.json ./
RUN npm ci

COPY tsconfig.json ./
COPY src ./src

CMD ["node", "src/job.js"]
