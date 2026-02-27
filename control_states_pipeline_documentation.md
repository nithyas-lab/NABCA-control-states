# NABCA Control States Pipeline - Complete Documentation

## Table of Contents

1. [Overview](#1-overview)
2. [PDF Report Structure](#2-pdf-report-structure)
3. [Database Schema Design](#3-database-schema-design)
4. [Data Extraction Pipeline](#4-data-extraction-pipeline)
5. [Table Type Classification](#5-table-type-classification)
6. [Record Generation Logic](#6-record-generation-logic)
7. [Commentary Extraction](#7-commentary-extraction)
8. [Data Cleaning & Transformation](#8-data-cleaning--transformation)
9. [Supabase Upload & Deduplication](#9-supabase-upload--deduplication)
10. [Pipeline Usage](#10-pipeline-usage)
11. [Environment Configuration](#11-environment-configuration)
12. [State Code Reference](#12-state-code-reference)
13. [Example Queries](#13-example-queries)
14. [Known Issues & Edge Cases](#14-known-issues--edge-cases)

---

## 1. Overview

The NABCA (National Alcoholic Beverage Control Association) Control States Pipeline extracts monthly sales data from PDF reports, processes it into structured records, and loads it into a Supabase PostgreSQL database.

**Data Coverage**: Monthly reports from control states (states where the government controls alcohol sales) — covering spirits and wine sales volume, dollar sales, and percent changes at both state and category levels.

**Pipeline Flow**:

```
PDFs in S3 (nabca-data/control-states/)
          |
          v
  AWS Textract (async document analysis)
          |
          v
  Table Extraction (grid parsing from Textract blocks)
  Text Extraction  (LINE blocks for commentary)
          |
          v
  Table Type Classification
  (wine / spirits_markets / spirits_on_premise / spirits_categories)
          |
          v
  Record Generation
  (each row -> 1 monthly + 1 rolling_12 record)
          |
          v
  Commentary Filtering
  (narrative text only, no headers/tables/numbers)
          |
          v
  JSON Output Files
  (sales_fact.json, brand_category_data.json, commentary.json)
          |
          v
  Supabase Upload (batched, with deduplication)
```

---

## 2. PDF Report Structure

Each monthly NABCA report is a single PDF file containing multiple data tables and narrative commentary.

### Filename Convention

Files are stored in S3 at `s3://nabca-data/control-states/` with names like:
- `CSResults_DEC2025_rev.pdf`
- `CSResults_NOV2025.pdf`
- `CSR_OCT2025.pdf`
- `CS_Results_JUL2024.pdf`

The pipeline parses `(year, month)` from filenames by finding month keywords (JAN, FEB, ... DEC) and a 4-digit year (`20XX`). The `_rev` suffix (revision) is stripped before parsing.

### Tables Inside Each PDF

Each PDF typically contains **4 data tables**:

| Table # | Type | Content | Destination Table |
|---------|------|---------|-------------------|
| 1 | Wine | Wine sales by state (17 control states + Total Control) | `sales_fact` |
| 2 | Spirits Markets (Total) | Total spirits sales by state | `sales_fact` |
| 3 | Spirits Categories | Spirit category breakdown (Vodka, Whiskey, etc.) | `brand_category_data` |
| 4 | Spirits Markets (On-Premise) | On-premise spirits sales by state | `sales_fact` |

### Table Row Layout

Each data table has a header row followed by data rows. Every data row contains **two report periods side by side**:

```
                    ┌─── Monthly Data ───┐  ┌─── Rolling 12-Month Data ───┐
                    │                    │  │                             │
State/Category    9L Vol   %Chg   $Sales  %Chg   9L Vol   %Chg    $Sales   %Chg   [PriceMix]
─────────────── ──────── ────── ──────── ─────  ──────── ────── ──────── ──────  ──────────
Alabama          307,189   2.8%  $67.0M   0.6%  3,589,927 -0.5%  $805.4M  -0.2%
Total Control   4,873,623  3.3%  $1.07B   1.1%  57,422,160 -2.3% $12.9B   -3.6%
```

- **Columns [0]**: Entity name (state name OR spirit category)
- **Columns [1-4]**: Monthly data (volume_9l, volume_pct_change, dollar_sales, dollar_pct_change)
- **Columns [5-8]**: Rolling 12-month data (same 4 metrics)
- **Column [9]** (categories table only): Price mix (rolling 12 only)

---

## 3. Database Schema Design

The pipeline stores data in **3 tables** within the `nabca` Supabase schema.

### Entity Relationship

```
┌─────────────────────┐
│     sales_fact       │  State-level sales (spirits + wine)
│─────────────────────│
│ id (PK)             │
│ year, month         │
│ report_date         │
│ report_type         │──── 'monthly' | 'rolling_12'
│ table_source        │──── 'spirits_markets' | 'spirits_on_premise' | 'wine'
│ channel             │──── 'total' | 'on_premise'
│ product_type        │──── 'spirits' | 'wine'
│ geography_type      │──── 'state' | 'total_control'
│ state_name          │──── Full state name (NULL for total_control)
│ volume_9l           │
│ volume_pct_change   │
│ dollar_sales        │
│ dollar_pct_change   │
│ created_at          │
└─────────────────────┘

┌─────────────────────┐
│ brand_category_data  │  Category-level breakdown (spirits only)
│─────────────────────│
│ id (PK)             │
│ year, month         │
│ report_date         │
│ report_type         │──── 'monthly' | 'rolling_12'
│ table_source        │──── Always 'spirits_categories'
│ channel             │──── Always 'total'
│ product_type        │──── Always 'spirits'
│ geography_type      │──── Always 'total_control'
│ category            │──── VODKA, WHISKEY, TEQUILA, RUM, GIN, ...
│ volume_9l           │
│ volume_pct_change   │
│ dollar_sales        │
│ dollar_pct_change   │
│ price_mix           │──── Rolling 12 only (NULL for monthly)
│ created_at          │
└─────────────────────┘

┌─────────────────────┐
│     commentary       │  Narrative text per report
│─────────────────────│
│ id (PK)             │
│ commentary_id (UQ)  │──── Format: NABCA-{year}-{month:02d}-001
│ year, month         │
│ report_date         │
│ section             │──── Always 'full_report'
│ content             │──── Full paragraph of narrative text
│ created_at          │
└─────────────────────┘
```

### Table Definitions (SQL)

#### sales_fact

```sql
CREATE TABLE nabca.sales_fact (
    id              BIGSERIAL PRIMARY KEY,
    year            INTEGER NOT NULL,
    month           INTEGER NOT NULL,
    report_date     DATE NOT NULL,               -- First of month: YYYY-MM-01
    report_type     TEXT NOT NULL                 -- 'monthly' or 'rolling_12'
                    CHECK (report_type IN ('monthly', 'rolling_12')),
    table_source    TEXT NOT NULL,                -- 'spirits_markets', 'spirits_on_premise', 'wine'
    channel         TEXT NOT NULL,                -- 'total' or 'on_premise'
    product_type    TEXT NOT NULL,                -- 'spirits' or 'wine'
    geography_type  TEXT NOT NULL,                -- 'state' or 'total_control'
    state_name      TEXT,                         -- Full name or NULL for aggregates
    volume_9l       NUMERIC,                      -- Volume in 9-liter case equivalents
    volume_pct_change NUMERIC,                    -- Year-over-year percent change
    dollar_sales    NUMERIC,                      -- Dollar amount
    dollar_pct_change NUMERIC,                    -- Year-over-year percent change
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
```

**Indexes**:
- `(year, month, report_type)` — time-based queries
- `(state_name)` — state-level filtering
- `(table_source, channel)` — source-level filtering

**Expected volume**: ~36 rows per table type per month x 3 table types x 2 report types = ~216 records/month

#### brand_category_data

```sql
CREATE TABLE nabca.brand_category_data (
    id              BIGSERIAL PRIMARY KEY,
    year            INTEGER NOT NULL,
    month           INTEGER NOT NULL,
    report_date     DATE NOT NULL,
    report_type     TEXT NOT NULL
                    CHECK (report_type IN ('monthly', 'rolling_12')),
    table_source    TEXT NOT NULL,                -- Always 'spirits_categories'
    channel         TEXT NOT NULL,                -- Always 'total'
    product_type    TEXT NOT NULL,                -- Always 'spirits'
    geography_type  TEXT NOT NULL,                -- Always 'total_control'
    category        TEXT NOT NULL,                -- Spirit category name
    volume_9l       NUMERIC,
    volume_pct_change NUMERIC,
    dollar_sales    NUMERIC,
    dollar_pct_change NUMERIC,
    price_mix       NUMERIC,                      -- Price mix % (rolling_12 only)
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
```

**Indexes**:
- `(year, month, report_type)` — time-based queries
- `(category)` — category filtering

**Expected volume**: ~10-12 categories x 2 report types = ~20-24 records/month

#### commentary

```sql
CREATE TABLE nabca.commentary (
    id              BIGSERIAL PRIMARY KEY,
    commentary_id   TEXT UNIQUE NOT NULL,         -- NABCA-{year}-{month:02d}-001
    year            INTEGER NOT NULL,
    month           INTEGER NOT NULL,
    report_date     DATE NOT NULL,
    section         TEXT NOT NULL,                -- Always 'full_report'
    content         TEXT NOT NULL,                -- Single paragraph of narrative text
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
```

**Indexes**:
- `(year, month)` — time-based queries
- `(commentary_id)` — unique lookups

**Expected volume**: 1 record per PDF (1 per month)

---

## 4. Data Extraction Pipeline

### Step 1: List PDFs from S3

The pipeline lists all `.pdf` files in `s3://{S3_BUCKET}/{S3_PREFIX}`. If specific months are requested (e.g., `2025-12`), it filters to only matching PDFs by parsing filenames.

### Step 2: AWS Textract (per PDF)

For each PDF, the pipeline:

1. **Starts async analysis**: `start_document_analysis()` with `FeatureTypes=['TABLES']`
2. **Polls for completion**: `get_document_analysis()` every 2 seconds, up to 60 attempts (120s timeout)
3. **Collects all blocks**: Handles pagination via `NextToken`

Textract returns blocks of types: `PAGE`, `TABLE`, `CELL`, `WORD`, `LINE`.

### Step 3: Parse Tables

`extract_tables_from_blocks(blocks)` converts Textract TABLE blocks into grid format:

```
TABLE block
  └── CHILD relationship → CELL blocks
       └── Each CELL has RowIndex, ColumnIndex
            └── CHILD relationship → WORD blocks (text content)
```

Result: list of tables, each a list of rows, each a list of cell strings.

### Step 4: Extract Text Lines

`extract_text_lines(blocks)` pulls all `LINE` blocks for commentary processing.

### Step 5: Classify Tables

Each table is classified by analyzing headers and first data rows (see Section 5).

### Step 6: Generate Records

Each classified table's data rows are parsed into monthly + rolling_12 records (see Section 6).

### Step 7: Output

- **JSON files**: `sales_fact.json`, `brand_category_data.json`, `commentary.json`
- **Supabase upload**: Batched insert with delete-before-insert deduplication

---

## 5. Table Type Classification

Function: `identify_table_type(table_rows, table_index)`

Classification rules are applied in priority order:

```
1. "WINE" in header?
   ├── YES → 'wine'
   └── NO → continue

2. "CATEGORIES" or "CATEGORY" in header?
   ├── YES → 'spirits_categories'
   └── NO → continue

3. Spirit category name in first data row?
   (VODKA, TEQUILA, WHISKEY, RUM, GIN, BRANDY, COGNAC, CORDIALS, COCKTAILS, CANADIAN)
   ├── YES → 'spirits_categories'
   └── NO → continue

4. "SPIRITS" or "MARKETS" in header, OR state name in first data row?
   (ALABAMA, TOTAL CONTROL)
   ├── YES → 'spirits_markets_{index}'
   └── NO → 'unknown'
```

**Post-classification reassignment**: When 2+ `spirits_markets_*` tables are found:
- **First** → `spirits_markets_total` (total channel)
- **Last** → `spirits_markets_on_premise` (on-premise channel)

### Mapping to Database Records

| Table Type | Destination | table_source | channel | product_type |
|------------|-------------|-------------|---------|-------------|
| `wine` | `sales_fact` | `wine` | `total` | `wine` |
| `spirits_markets_total` | `sales_fact` | `spirits_markets` | `total` | `spirits` |
| `spirits_markets_on_premise` | `sales_fact` | `spirits_on_premise` | `on_premise` | `spirits` |
| `spirits_categories` | `brand_category_data` | `spirits_categories` | `total` | `spirits` |

---

## 6. Record Generation Logic

Function: `parse_table_row(row, year, month, report_date, table_source, channel, product_type)`

**Each data row generates exactly 2 records** — one `monthly` and one `rolling_12`.

### For sales_fact rows (wine + spirits markets)

**Input row** (9 columns):

| Col | Content | Monthly Record Field | Rolling_12 Record Field |
|-----|---------|---------------------|------------------------|
| [0] | Entity name | state_name | state_name |
| [1] | Monthly 9L volume | volume_9l | — |
| [2] | Monthly volume % change | volume_pct_change | — |
| [3] | Monthly dollar sales | dollar_sales | — |
| [4] | Monthly dollar % change | dollar_pct_change | — |
| [5] | Rolling 9L volume | — | volume_9l |
| [6] | Rolling volume % change | — | volume_pct_change |
| [7] | Rolling dollar sales | — | dollar_sales |
| [8] | Rolling dollar % change | — | dollar_pct_change |

**Geography logic**:
- Entity = "Total Control" → `geography_type='total_control'`, `state_name=NULL`
- Any other entity → `geography_type='state'`, `state_name` = entity name

### For brand_category_data rows (spirits categories)

**Input row** (9-10 columns):

Same column layout as sales_fact, plus:
| Col | Content | Monthly Record Field | Rolling_12 Record Field |
|-----|---------|---------------------|------------------------|
| [9] | Price mix (optional) | price_mix = NULL | price_mix = value |

**Fixed values**: `geography_type='total_control'`, `category` = entity name

### Minimum Row Length

Rows with fewer than 9 columns or empty entity names are skipped (returns empty list).

---

## 7. Commentary Extraction

Function: `extract_commentary(text_lines)`

Filters all text lines from the PDF to keep only **narrative paragraph text**, discarding headers, table data, and structural elements.

### Filtering Rules (applied in order)

| Filter | Pattern | What it removes | Example |
|--------|---------|-----------------|---------|
| Headers/Footers | `^NABCA Monthly Report`, `Control States Results$`, `^\d+$`, `^Page \d+` | Report headers, page numbers | "NABCA Monthly Report", "5", "Page 10" |
| Table data | `\d+,\d+.*\d+,\d+.*\d+,\d+` | Rows with 3+ formatted numbers | "Alabama 307,189 2.8% 67,074,752" |
| Column headers | Length < 50 AND contains "9L", "CMTY", "R12TY", "Shelf $" | Table column labels | "9L CMTY R12TY Shelf $" |
| State names alone | Length < 40 AND contains any state name | Standalone state labels | "Alabama" |
| Numeric-only | `^[0-9$%\s,\-\.]+$` | Lines that are only numbers/symbols | "$123,456", "-0.8%" |
| Too short | Length <= 40 | Lines too short to be narrative | "Table 2" |

### Output

- All surviving lines are joined with spaces into a **single paragraph**
- Minimum 50 characters required to create a commentary record
- One commentary record per PDF with `section='full_report'`
- Commentary ID format: `NABCA-{year}-{month:02d}-001` (e.g., `NABCA-2025-12-001`)

---

## 8. Data Cleaning & Transformation

### clean_value(value)

Converts PDF string values to Python numeric types:

```python
def clean_value(value):
    # Step 1: Handle empty/null
    if not value or value.strip() == '':
        return None

    # Step 2: Strip currency/percentage symbols
    val = value.strip().replace('$', '').replace(',', '').replace('%', '')

    # Step 3: Convert to int or float
    return int(val) if '.' not in val else float(val)

    # Step 4: Invalid values return None
```

**Conversion examples**:

| Input | Output | Type |
|-------|--------|------|
| `"4,873,623"` | `4873623` | int |
| `"$1,074,541,845"` | `1074541845` | int |
| `"3.3%"` | `3.3` | float |
| `"-0.5%"` | `-0.5` | float |
| `"$59.99"` | `59.99` | float |
| `""` | `None` | NoneType |
| `"N/A"` | `None` | NoneType |

---

## 9. Supabase Upload & Deduplication

### Upload Strategy

Records are uploaded in batches:
- **sales_fact** and **brand_category_data**: 100 records per batch
- **commentary**: 50 records per batch

### Deduplication (Delete-Before-Insert)

To prevent duplicates on re-runs, the pipeline **deletes all existing records** for the processed month(s) before inserting:

```
For each (year, month) being processed:
  1. DELETE FROM sales_fact WHERE year = {year} AND month = {month}
  2. DELETE FROM brand_category_data WHERE year = {year} AND month = {month}
  3. DELETE FROM commentary WHERE year = {year} AND month = {month}
  4. INSERT new records
```

This makes the pipeline **idempotent** — safe to re-run for any month without accumulating duplicates.

### Error Handling

- Failed batches are counted but don't stop the pipeline
- Final summary reports uploaded vs failed counts
- Supabase client initialization validates credentials are set

---

## 10. Pipeline Usage

### Basic Commands

```bash
# Process ALL PDFs in S3 bucket
python control_states_pipeline.py

# Process specific month(s)
python control_states_pipeline.py 2025-12
python control_states_pipeline.py 2025-10 2025-11 2025-12

# Extract only (save JSON, skip Supabase upload)
python control_states_pipeline.py --no-upload

# Extract specific months, save to custom directory
python control_states_pipeline.py 2025-12 --no-upload --output-dir ./results
```

### Output Files

The pipeline always saves JSON files (regardless of upload):
- `sales_fact.json` — all sales records
- `brand_category_data.json` — all category records
- `commentary.json` — all commentary records

### Dependencies

```bash
pip install boto3 supabase
```

- **boto3**: AWS SDK (S3 listing, Textract API)
- **supabase**: Supabase Python client (database uploads)

---

## 11. Environment Configuration

All configuration is via environment variables (no hardcoded credentials):

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `AWS_ACCESS_KEY_ID` | Yes | — | AWS access key for S3 and Textract |
| `AWS_SECRET_ACCESS_KEY` | Yes | — | AWS secret key |
| `AWS_REGION` | No | `us-east-1` | AWS region |
| `S3_BUCKET` | No | `nabca-data` | S3 bucket containing PDFs |
| `S3_PREFIX` | No | `control-states/` | S3 folder prefix for PDFs |
| `SUPABASE_URL` | Yes* | — | Supabase project URL |
| `SUPABASE_KEY` | Yes* | — | Supabase anon or service role key |
| `SUPABASE_SCHEMA` | No | `nabca` | Supabase schema name |

*Required only when uploading (not needed with `--no-upload`)

### Example .env File

```env
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=wJalr...
AWS_REGION=us-east-1
S3_BUCKET=nabca-data
S3_PREFIX=control-states/
SUPABASE_URL=https://yourproject.supabase.co
SUPABASE_KEY=eyJhbGci...
SUPABASE_SCHEMA=nabca
```

---

## 12. State Code Reference

Control states covered in the reports:

| State Name | Code | Notes |
|-----------|------|-------|
| Alabama | AL | |
| Idaho | ID | |
| Iowa | IA | |
| Maine | ME | |
| Michigan | MI | |
| Mississippi | MS | |
| Mont Co | MD | Montgomery County, Maryland |
| Montana | MT | |
| New Hampshire | NH | |
| North Carolina | NC | |
| Ohio | OH | |
| Oregon | OR | |
| Pennsylvania | PA | |
| Utah | UT | |
| Vermont | VT | |
| Virginia | VA | |
| West Virginia | WV | Also handles "West Virgina" typo in PDFs |
| Wyoming | WY | |
| Total Control | — | Aggregate of all control states |

**Note**: The database stores full state names (e.g., "Alabama"), not two-letter codes.

---

## 13. Example Queries

### Monthly spirits sales for a specific state

```sql
SELECT year, month, volume_9l, dollar_sales, volume_pct_change
FROM nabca.sales_fact
WHERE state_name = 'Pennsylvania'
  AND report_type = 'monthly'
  AND table_source = 'spirits_markets'
ORDER BY year, month;
```

### Rolling 12-month category performance

```sql
SELECT year, month, category, volume_9l, dollar_sales, price_mix
FROM nabca.brand_category_data
WHERE report_type = 'rolling_12'
  AND year = 2025 AND month = 12
ORDER BY dollar_sales DESC;
```

### Total control vs on-premise comparison

```sql
SELECT year, month,
       SUM(CASE WHEN channel = 'total' THEN volume_9l END) as total_vol,
       SUM(CASE WHEN channel = 'on_premise' THEN volume_9l END) as on_premise_vol
FROM nabca.sales_fact
WHERE report_type = 'monthly'
  AND product_type = 'spirits'
  AND geography_type = 'total_control'
GROUP BY year, month
ORDER BY year, month;
```

### Commentary for a specific month

```sql
SELECT content
FROM nabca.commentary
WHERE year = 2025 AND month = 12;
```

### Wine vs spirits volume trend

```sql
SELECT year, month, product_type,
       SUM(volume_9l) as total_volume
FROM nabca.sales_fact
WHERE report_type = 'monthly'
  AND geography_type = 'total_control'
GROUP BY year, month, product_type
ORDER BY year, month;
```

### Validate record counts per month

```sql
SELECT year, month,
       COUNT(*) FILTER (WHERE report_type = 'monthly') as monthly_count,
       COUNT(*) FILTER (WHERE report_type = 'rolling_12') as rolling_count
FROM nabca.sales_fact
GROUP BY year, month
ORDER BY year, month;
```

---

## 14. Known Issues & Edge Cases

### PDF Filename Variations
- Some PDFs use `CSResults_`, others use `CSR_` or `CS_Results_`
- Revision suffix `_rev` is stripped before parsing
- All variations are handled by the month keyword + year regex parser

### Commentary Minimum Length
- Commentary records are only created if the extracted text exceeds 50 characters
- Some PDFs may have very little or no narrative text, resulting in no commentary record

### Numeric Parsing
- `clean_value()` handles `$`, `,`, `%` symbols but not OCR artifacts like misread characters
- Invalid values return `None` (stored as NULL in database)

### No UNIQUE Constraints on sales_fact and brand_category_data
- These tables do not have UNIQUE constraints to prevent duplicate rows
- The pipeline handles deduplication by deleting existing month data before re-inserting
- If uploaded outside the pipeline without deletion, duplicates can accumulate

### Total Control Aggregate
- "Total Control" is an aggregate row appearing in each state-level table
- Stored with `geography_type='total_control'` and `state_name=NULL`
- Not a simple sum of individual states (may include data from states not individually listed)

### Price Mix Field
- Only populated for `rolling_12` records in `brand_category_data`
- Always NULL for `monthly` records
- Column [9] in the categories table (may not exist in every row)
