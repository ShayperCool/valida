FROM oven/bun:1.4.1
WORKDIR /app
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile
COPY . .
ENV HOST=0.0.0.0 PORT=2026 NODE_ENV=production
EXPOSE 2026
CMD ["bun", "src/main.ts"]
