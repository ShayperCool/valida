FROM oven/bun:1.4.1 AS build
WORKDIR /app
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile
COPY ui/package.json ui/bun.lock ./ui/
RUN bun --cwd ui install --frozen-lockfile
COPY . .
RUN bun --cwd ui run build

FROM oven/bun:1.4.1
WORKDIR /app
COPY --from=build /app /app
ENV HOST=0.0.0.0 PORT=2026 NODE_ENV=production
EXPOSE 2026
CMD ["bun", "src/main.ts"]
