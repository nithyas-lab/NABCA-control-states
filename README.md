# NABCA Control States Pipeline

Extracts monthly sales data from NABCA Control States PDF reports using AWS Textract and loads it into Supabase PostgreSQL.

## What It Does

- Reads PDF reports from S3 (`nabca-data/control-states/`)
- Extracts 4 table types: wine sales, spirits sales (total + on-premise), spirit categories
- Generates monthly and rolling 12-month records from each table row
- Extracts narrative commentary text (filtered from headers/tables/numbers)
- Uploads structured data to 3 Supabase tables: `sales_fact`, `brand_category_data`, `commentary`

## Quick Start

```bash
pip install boto3 supabase

# Set environment variables (see below)
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export SUPABASE_URL=...
export SUPABASE_KEY=...

# Process all PDFs
python control_states_pipeline.py

# Process specific month(s)
python control_states_pipeline.py 2025-12

# Extract only (no Supabase upload)
python control_states_pipeline.py --no-upload
```

## Files

| File | Description |
|------|-------------|
| `control_states_pipeline.py` | Main pipeline script |
| `supabase_schema.sql` | Database schema DDL (run in Supabase SQL Editor before first upload) |
| `control_states_pipeline_documentation.md` | Complete documentation (schema design, extraction logic, examples) |

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `AWS_ACCESS_KEY_ID` | Yes | - | AWS access key |
| `AWS_SECRET_ACCESS_KEY` | Yes | - | AWS secret key |
| `AWS_REGION` | No | `us-east-1` | AWS region |
| `S3_BUCKET` | No | `nabca-data` | S3 bucket name |
| `S3_PREFIX` | No | `control-states/` | S3 folder prefix |
| `SUPABASE_URL` | Yes* | - | Supabase project URL |
| `SUPABASE_KEY` | Yes* | - | Supabase service/anon key |
| `SUPABASE_SCHEMA` | No | `nabca` | Target schema |

*Required only when uploading (not needed with `--no-upload`)

## Database Schema

Three tables in the `nabca` schema:

- **sales_fact** — State-level sales data (spirits + wine), monthly and rolling 12-month
- **brand_category_data** — Spirit category breakdown (Vodka, Whiskey, etc.), includes price_mix
- **commentary** — Narrative text extracted from each report (one record per PDF)

See `control_states_pipeline_documentation.md` for complete schema details, column definitions, and example queries.
