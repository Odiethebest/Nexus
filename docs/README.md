# Nexus Documentation Index

This directory contains canonical project documentation and operational runbooks.

## Document Map

- [ARCHITECTURE.md](./ARCHITECTURE.md): System architecture, service boundaries, and message flow
- [RUN_LOCAL.md](./RUN_LOCAL.md): Local development and integration runbook
- [DEPLOYMENT.md](./DEPLOYMENT.md): Deployment strategy for Docker Compose and Railway
- [ENVIRONMENT.md](./ENVIRONMENT.md): Environment variable reference and ownership

## Documentation Standards

- Keep behavioral and API changes synchronized with the relevant docs.
- Update both `ENVIRONMENT.md` and `.env.example` when introducing or removing configuration keys.
- Keep the root `README.md` concise; put detailed operational guidance under `docs/`.
