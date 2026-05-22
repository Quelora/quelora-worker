# quelora-worker

**Background job worker for the [Quelora](https://github.com/Quelora) platform.**

[![License: AGPL-3.0](https://img.shields.io/badge/License-AGPL--3.0-blue.svg)](./LICENSE)

A BullMQ worker process that consumes the real-time, fire-and-forget side
effects produced by the Quelora APIs, so user requests never block on them.

## Queues consumed

| Queue | Work |
|-------|------|
| `emails` | Transactional and broadcast email delivery |
| `notifications` | Web Push notification fan-out |
| `activity` | Batch persistence of the user activity log |
| `aggregation` | Counter and stats aggregation |

## Requirements

- Node.js 20+ · MongoDB 4.4+ · Redis 6+ (BullMQ broker)

## Setup

```bash
npm install
# configure the environment (see CACHE_REDIS_URL, MONGO_URI, SMTP, …)
npm start
```

## Architecture

Depends on [`@quelora/common`](https://github.com/Quelora/quelora-common) for
models, services and the BullMQ infrastructure. Jobs are enqueued by
[`quelora-public-api`](https://github.com/Quelora/quelora-public-api) and
[`quelora-dashboard-api`](https://github.com/Quelora/quelora-dashboard-api).
Scheduled (cron-style) jobs are handled separately by
[`quelora-jobs`](https://github.com/Quelora/quelora-jobs).

## License

[AGPL-3.0-only](./LICENSE) — Copyright (C) 2026 Germán Zelaya.

Part of the **[Quelora](https://github.com/Quelora)** project.
