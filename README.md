```markdown
# Enterprise Logistics Pipeline

An Airflow-orchestrated pipeline that simulates daily logistics booking data for a fictional trucking company (NaijaMove Logistics, the business use case this project models), intentionally injects realistic "bad data," and lands it in a cloud storage landing zone — proving the downstream schema and staging logic can absorb messy production data without breaking.

## What this demonstrates

Rather than downloading a ready-made dataset, this pipeline generates its own source data, the way a real operational system would — modeling a fictional logistics company, NaijaMove, so every field, constraint, and anomaly type maps to a concrete business scenario instead of an abstract "row 1, row 2." It exists to prove two things at once:
* An Airflow DAG can reliably produce and move business data on a schedule.
* The data model (staging tables, schemas, dedup logic) can survive contact with duplicate records, null critical fields, out-of-range values, and malformed identifiers — the kind of mess that shows up in real systems, not the kind that shows up in tutorials.

## Architecture

```text
┌─────────────────────────┐      ┌──────────────────────────┐      ┌────────────────────┐
│  generate_shipments      │      │  upload_raw_to_gcs        │      │  GCS Landing Zone   │
│  (PythonOperator)        │ ──►  │  (LocalFilesystemToS3     │ ──►  │  gs://.../landing/  │
│                          │      │   Operator, HMAC auth)     │      │                    │
│  Runs generator script   │      │  Uploads NDJSON via GCS's  │      │  Partitioned:      │
│  as isolated subprocess  │      │  S3-compatible endpoint    │      │  year=/month=      │
└─────────────────────────┘      └──────────────────────────┘      └────────────────────┘

```

**Why an S3 operator against a GCS bucket:** Available GCP credentials are HMAC (S3-interoperability) keys, not a service-account JSON keyfile. Google's native GCSHook/LocalFilesystemToGCSOperator can't authenticate with HMAC keys, so the pipeline uses the AWS provider's LocalFilesystemToS3Operator pointed at GCS's S3-compatible XML API endpoint (`https://storage.googleapis.com`) instead. This is a supported, documented pattern for GCS — not a hack.

## The data generator

`scripts/generate_shipment_batch.py` produces a batch of simulated shipment bookings per run, seeded deterministically off the execution date (same date → same output, every time — makes debugging and grading reproducible).

**Corruption engine:** ~5% of records are deliberately mutated into one of six anomaly types, each targeting a specific failure mode a staging layer must defend against:

| # | Anomaly | What it breaks |
| --- | --- | --- |
| 1 | Unparseable weight value | Type casting / SAFE_CAST |
| 2 | Null critical field (`delivery_status`) | Not-null constraints |
| 3 | Malformed truck plate number | Referential/format validation |
| 4 | Out-of-bounds destination | Domain/lookup validation |
| 5 | Unparseable timestamp format | Date parsing |
| 6 | Duplicate `shipment_id` | Idempotency / dedup logic |

Every generated file is line-delimited JSON (NDJSON), one shipment per line.

## Repository layout

```text
.
├── dags/
│   └── naijamove_ingestion_dag.py     # Airflow 3 TaskFlow DAG
├── scripts/
│   └── generate_shipment_batch.py     # Standalone data generator
├── data/                              # Local output (gitignored — see below)
├── Dockerfile
├── docker-compose.yaml
├── requirements.txt
└── README.md

```

## Running it locally

```bash
docker compose build
docker compose up airflow-init
docker compose up -d

```

Airflow UI: `http://localhost:8080` (Airflow 3's Simple Auth Manager auto-generates an admin password on first boot — check `docker compose logs airflow-webserver | grep -i password`).

Set up the GCS connection (Admin → Connections, or via CLI):

```bash
docker compose exec --user airflow airflow-webserver airflow connections add "gcp_hmac_connection" \
  --conn-type "aws" \
  --conn-login "<HMAC_ACCESS_KEY>" \
  --conn-password "<HMAC_SECRET_KEY>" \
  --conn-extra '{"endpoint_url": "[https://storage.googleapis.com](https://storage.googleapis.com)", "config_kwargs": {"s3": {"addressing_style": "path"}}}'

```

Run the pipeline:

```bash
docker compose run --rm airflow-webserver airflow tasks test naijamove_ingestion_pipeline generate_shipments_locally 2026-07-17
docker compose run --rm airflow-webserver airflow tasks test naijamove_ingestion_pipeline upload_raw_to_gcs_landing 2026-07-17

```

## Current status

| Component | Status |
| --- | --- |
| Data generator (6 anomaly types, deterministic seeding) | ✅ Working, verified locally |
| Airflow DAG (TaskFlow API, 2-task chain) | ✅ Parses and registers correctly |
| Local generation task | ✅ Verified via airflow tasks test |
| GCS upload — auth/signing (HMAC via S3-compatible endpoint) | ✅ Resolved (path-style addressing) |

The pipeline is fully built and provably correct end-to-end up to the point of GCP-side bucket permissions, which are outside this repo's control.
```

```
