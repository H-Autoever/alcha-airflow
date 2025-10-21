# Airflow Docker Project

This repository contains an Apache Airflow setup (Docker Compose) and example DAGs
for Redshift COPY loads and a Redshift → MySQL export.

## Structure

- `docker-compose.yaml` — Airflow + Postgres + Redis for local development
- `dags/` — DAGs, helper libs, and SQL templates
  - `dags/sql/` — SQL grouped by purpose (`connect/`, `init/`, `copy/`, `query/`)
  - `dags/lib/` — Python helpers used by DAGs
- `plugins/` — Airflow plugins (empty by default)
- `config/` — Optional Airflow config overrides (not required)
- `logs/` — Airflow logs (ignored by Git)
- `infra/aws/` — One‑time AWS setup scripts/policies for Redshift + S3 access
- `.env.example` — Example environment variables for Docker Compose
- `.gitignore` — Ignores logs, caches, secrets, local IDE files, etc.

## Usage

1) Copy `.env.example` to `.env` and adjust values if needed.

2) Start the stack:

```
docker compose up -d
```

3) Open the Airflow UI: http://localhost:8080 (default: `airflow` / `airflow`).

4) Set connections in Airflow (UI -> Admin -> Connections):
- `redshift` (Postgres‑compatible Redshift connection)
- `mysql_local` (MySQL connection used by the export task, created in UI)

Alternatively, you can configure connections via environment variables or a
connections JSON, but the UI is simplest for local dev.

## One‑Time AWS Setup (optional)

For Redshift Serverless to read from S3 (COPY from parquet, etc.), see `infra/aws/`:
- `infra/aws/setup_redshift_iam_role.sh` — creates/updates IAM role and sets it as
  the Default IAM Role for a Redshift Serverless namespace
- `infra/aws/policies/` — example trust policy and bucket policy template
- `infra/aws/examples/select/` — sample S3 Select serialization configs

These are not required at runtime by the containers; they’re just operational
docs/scripts to bootstrap AWS permissions.

## Notes

- Do not commit secrets. Use `.env` (ignored) or Airflow’s secret backends.
- `logs/` and `__pycache__/` are ignored. If any cache files were present, they’ve been removed.
- If you extend Python dependencies, prefer building a custom image instead of
  using `_PIP_ADDITIONAL_REQUIREMENTS` for anything beyond quick checks.
