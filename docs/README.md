# Nexus Documentation Index

This directory contains canonical project documentation and operational runbooks.

## Document Map

- [ARCHITECTURE.md](./ARCHITECTURE.md): System architecture, topology, message flow, failure handling
- [RUN_LOCAL.md](./RUN_LOCAL.md): Local development and integration runbook
- [DEPLOYMENT.md](./DEPLOYMENT.md): Deployment strategy for Docker Compose and Railway
- [ENVIRONMENT.md](./ENVIRONMENT.md): Environment variable reference and ownership
- [STRUCTURE.md](./STRUCTURE.md): Repository layout

Outside this directory:

- [../README.md](../README.md): project overview and quick start
- [../RUNBOOK.md](../RUNBOOK.md): claim → code → metric → reproduction mapping
- [../MIGRATION.md](../MIGRATION.md): RabbitMQ → Redpanda transition record
- [../CLAUDE.md](../CLAUDE.md): full project specification

## Documentation Standards

- Keep behavioral and API changes synchronized with the relevant docs.
- Update both `ENVIRONMENT.md` and `.env.example` when introducing or removing configuration keys.
- Keep the root `README.md` concise; put detailed operational guidance under `docs/`.
