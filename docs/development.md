---
layout: page
title: Development
permalink: /development.html
---

## Prerequisites

- Node.js and pnpm (`mise install` pins the pnpm version CI uses)
- Docker with Compose, to run the examples against hatchet-lite

## Install

```sh
pnpm install
```

## Run locally

Start Hatchet and PostgreSQL, then obtain a local client token:

```sh
docker compose up -d
./scripts/hatchet-token.sh
export HATCHET_CLIENT_TOKEN=$(cat .hatchet-token)
export HATCHET_CLIENT_TLS_STRATEGY=none
```

The `FromConfig` layers load these values through Effect `Config`. Optional
settings accept Effect duration strings:

| Variable | Default |
| --- | --- |
| `HATCHET_CLIENT_HOST_PORT` | Hatchet SDK default |
| `HATCHET_IDEMPOTENCY_FALLBACK_TTL` | `24 hours` |
| `HATCHET_RESULT_POLL_INTERVAL` | `300 millis` |

Run the worker and client in separate terminals:

```sh
pnpm worker
```

```sh
pnpm main
```

The local Hatchet dashboard is available at <http://localhost:8888>.

## Verify changes

```sh
pnpm typecheck
pnpm test
```

The end-to-end suite runs against a real Hatchet engine:

```sh
pnpm test:e2e
```

With `HATCHET_CLIENT_TOKEN` set or a `.hatchet-token` file present, it uses
that engine, such as the docker-compose hatchet-lite above. Otherwise
`test/e2e.setup.ts` starts Hatchet's embedded engine with a bundled Postgres.
The engine binary is downloaded and cached under `~/.hatchet/embedded` on first
use, so the suite needs neither Docker nor a token. CI runs it this way.

## Project layout

```text
src/
  HatchetWorkflowEngine.ts  WorkflowEngine implementation
  HatchetWorker.ts          scoped Hatchet worker Layer
  internal/                 codecs and infrastructure errors
example/
  erp/                      backend-agnostic Effect workflows and services
  Main.ts                   client bootstrap
  Worker.ts                 worker bootstrap
test/
  HatchetWorkflowEngine.test.ts
  e2e.test.ts
  e2e.setup.ts              embedded engine for the e2e suite
```
