# AL-EP

Automated scheduled data indexing and metadata synchronization pipeline.

## Setup

1. Fork or clone this repository.
2. Add required environment secrets under **Settings → Secrets and variables → Actions**.
3. The workflow runs automatically on schedule. You can also trigger it manually from the **Actions** tab.

## Required Secrets

| Name | Description |
|------|-------------|
| `TURSO_URL` | Database endpoint URL |
| `TURSO_TOKEN` | Database auth token |
| `PIXELDRAIN_API_KEY` | Storage provider API key |
| `GAS_PROXY_URL` | Proxy gateway URL |
