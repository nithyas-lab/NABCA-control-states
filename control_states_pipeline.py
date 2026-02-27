"""
NABCA Control States Pipeline

Extracts sales data, brand/category data, and commentary from NABCA monthly
PDF reports using AWS Textract, then uploads to Supabase.

Usage:
  # Process ALL PDFs in S3 bucket
  python control_states_pipeline.py

  # Process specific month(s)
  python control_states_pipeline.py 2025-12
  python control_states_pipeline.py 2025-10 2025-11 2025-12

  # Process all but skip upload (extract only, save JSON)
  python control_states_pipeline.py --no-upload

  # Process specific months, extract only
  python control_states_pipeline.py 2025-12 --no-upload

Environment Variables (required):
  AWS_ACCESS_KEY_ID       AWS access key
  AWS_SECRET_ACCESS_KEY   AWS secret key
  AWS_REGION              AWS region (default: us-east-1)
  S3_BUCKET               S3 bucket name (default: nabca-data)
  S3_PREFIX               S3 folder prefix (default: control-states/)
  SUPABASE_URL            Supabase project URL
  SUPABASE_KEY            Supabase anon/service key
  SUPABASE_SCHEMA         Supabase schema (default: nabca)
"""

import boto3
import time
import json
import os
import re
import sys
import argparse
from botocore.exceptions import ClientError

# ============================================================
# Configuration (all from environment variables)
# ============================================================

AWS_ACCESS_KEY = os.environ.get('AWS_ACCESS_KEY_ID', '')
AWS_SECRET_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY', '')
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')
S3_BUCKET = os.environ.get('S3_BUCKET', 'nabca-data')
S3_PREFIX = os.environ.get('S3_PREFIX', 'control-states/')

SUPABASE_URL = os.environ.get('SUPABASE_URL', '')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', '')
SUPABASE_SCHEMA = os.environ.get('SUPABASE_SCHEMA', 'nabca')

# ============================================================
# Constants
# ============================================================

STATE_CODES = {
    'Alabama': 'AL', 'Iowa': 'IA', 'Idaho': 'ID', 'Mont Co': 'MD', 'Maine': 'ME',
    'Michigan': 'MI', 'Mississippi': 'MS', 'Montana': 'MT', 'North Carolina': 'NC',
    'New Hampshire': 'NH', 'Ohio': 'OH', 'Oregon': 'OR', 'Pennsylvania': 'PA',
    'Utah': 'UT', 'Virginia': 'VA', 'Vermont': 'VT', 'West Virginia': 'WV',
    'West Virgina': 'WV', 'Wyoming': 'WY', 'Total Control': None
}

MONTH_MAP = {
    'JAN': 1, 'JANUARY': 1, 'FEB': 2, 'FEBRUARY': 2, 'MAR': 3, 'MARCH': 3,
    'APR': 4, 'APRIL': 4, 'MAY': 5, 'JUN': 6, 'JUNE': 6, 'JUL': 7, 'JULY': 7,
    'AUG': 8, 'AUGUST': 8, 'SEPT': 9, 'SEPTEMBER': 9, 'OCT': 10, 'OCTOBER': 10,
    'NOV': 11, 'NOVEMBER': 11, 'DEC': 12, 'DECEMBER': 12
}

SPIRIT_CATEGORIES = [
    'VODKA', 'TEQUILA', 'WHISKEY', 'RUM', 'GIN', 'BRANDY',
    'COGNAC', 'CORDIALS', 'COCKTAILS', 'CANADIAN'
]


# ============================================================
# AWS Helpers
# ============================================================

def init_aws_clients():
    """Initialize AWS clients. Validates credentials are set."""
    if not AWS_ACCESS_KEY or not AWS_SECRET_KEY:
        print("ERROR: AWS credentials not set.")
        print("  Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables.")
        sys.exit(1)

    textract = boto3.client(
        'textract',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION
    )
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION
    )
    return textract, s3


def list_s3_pdfs(s3_client, target_months=None):
    """List PDF files from S3. Optionally filter by target months.

    Args:
        s3_client: boto3 S3 client
        target_months: set of (year, month) tuples to filter, or None for all
    Returns:
        list of S3 keys
    """
    try:
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_PREFIX)
        if 'Contents' not in response:
            return []
        all_pdfs = sorted([obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.pdf')])
    except Exception as e:
        print(f"  ERROR listing S3 bucket: {e}")
        return []

    if target_months is None:
        return all_pdfs

    # Filter to only requested months
    filtered = []
    for key in all_pdfs:
        filename = os.path.basename(key)
        year, month = parse_filename(filename)
        if year and month and (year, month) in target_months:
            filtered.append(key)
    return filtered


def start_textract(textract_client, s3_key):
    """Start async Textract document analysis."""
    try:
        response = textract_client.start_document_analysis(
            DocumentLocation={'S3Object': {'Bucket': S3_BUCKET, 'Name': s3_key}},
            FeatureTypes=['TABLES']
        )
        return response['JobId']
    except Exception as e:
        print(f"    ERROR starting Textract: {e}")
        return None


def get_textract_results(textract_client, job_id):
    """Poll for Textract results. Returns (success, blocks_or_error)."""
    for attempt in range(60):
        try:
            response = textract_client.get_document_analysis(JobId=job_id)
            if response['JobStatus'] == 'SUCCEEDED':
                blocks = response['Blocks']
                while response.get('NextToken'):
                    response = textract_client.get_document_analysis(
                        JobId=job_id, NextToken=response['NextToken']
                    )
                    blocks.extend(response['Blocks'])
                return True, blocks
            elif response['JobStatus'] == 'FAILED':
                return False, "Textract job failed"
            time.sleep(2)
        except Exception as e:
            return False, f"Error polling Textract: {e}"
    return False, "Textract timeout (120s)"


# ============================================================
# PDF Parsing
# ============================================================

def parse_filename(filename):
    """Extract (year, month) from filename like CSResults_DEC2025_rev.pdf"""
    name = filename.replace('.pdf', '').replace('_rev', '')
    for month_str, month_num in MONTH_MAP.items():
        if month_str in name.upper():
            year_match = re.search(r'20\d{2}', name)
            if year_match:
                return int(year_match.group()), month_num
    return None, None


def clean_value(value):
    """Clean numeric value -- strips $, commas, %, converts to int/float."""
    if not value or value.strip() == '':
        return None
    val = value.strip().replace('$', '').replace(',', '').replace('%', '')
    try:
        return int(val) if '.' not in val else float(val)
    except (ValueError, TypeError):
        return None


# ============================================================
# Textract Table Extraction
# ============================================================

def extract_tables_from_blocks(blocks):
    """Parse Textract blocks into list of table grids (list of list of strings)."""
    tables = []
    table_blocks = [b for b in blocks if b['BlockType'] == 'TABLE']

    for table_block in table_blocks:
        if 'Relationships' not in table_block:
            continue
        cell_map = {}
        for rel in table_block['Relationships']:
            if rel['Type'] == 'CHILD':
                for cell_id in rel['Ids']:
                    cell = next((b for b in blocks if b['Id'] == cell_id), None)
                    if not cell or cell['BlockType'] != 'CELL':
                        continue
                    row_idx = cell.get('RowIndex', 0)
                    col_idx = cell.get('ColumnIndex', 0)
                    cell_text = ""
                    if 'Relationships' in cell:
                        for cell_rel in cell['Relationships']:
                            if cell_rel['Type'] == 'CHILD':
                                for word_id in cell_rel['Ids']:
                                    word = next((b for b in blocks if b['Id'] == word_id), None)
                                    if word and word['BlockType'] == 'WORD':
                                        cell_text += word['Text'] + " "
                    if row_idx not in cell_map:
                        cell_map[row_idx] = {}
                    cell_map[row_idx][col_idx] = cell_text.strip()

        rows = []
        for row_idx in sorted(cell_map.keys()):
            row = [cell_map[row_idx].get(col_idx, '') for col_idx in sorted(cell_map[row_idx].keys())]
            rows.append(row)
        if rows:
            tables.append(rows)
    return tables


def extract_text_lines(blocks):
    """Extract all text lines from Textract blocks."""
    return [block['Text'] for block in blocks if block['BlockType'] == 'LINE']


def identify_table_type(table_rows, table_index):
    """Classify a table by inspecting header and first data row."""
    if not table_rows or len(table_rows) < 2:
        return None
    header = ' '.join(table_rows[0]).upper()
    first_data = ' '.join(table_rows[1]).upper() if len(table_rows) > 1 else ''

    if 'WINE' in header:
        return 'wine'
    if 'CATEGORIES' in header or 'CATEGORY' in header:
        return 'spirits_categories'
    if any(cat in first_data for cat in SPIRIT_CATEGORIES):
        return 'spirits_categories'
    if 'SPIRITS' in header or 'MARKETS' in header or \
       any(state in first_data for state in ['ALABAMA', 'TOTAL CONTROL']):
        return f'spirits_markets_{table_index}'
    return 'unknown'


# ============================================================
# Record Building
# ============================================================

def parse_table_row(row, year, month, report_date, table_source, channel, product_type):
    """Parse one table row into 2 records (monthly + rolling_12)."""
    if len(row) < 9:
        return []

    entity_name = row[0].strip()
    if not entity_name:
        return []

    is_total = entity_name == 'Total Control'
    is_category = table_source == 'spirits_categories'

    geography_type = 'total_control' if is_total else ('state' if not is_category else 'total_control')
    state_name = None if (is_total or is_category) else entity_name
    category = entity_name if is_category else None

    records = []

    if is_category:
        monthly = {
            'year': year, 'month': month, 'report_date': report_date,
            'report_type': 'monthly', 'table_source': table_source,
            'channel': channel, 'product_type': product_type,
            'geography_type': geography_type, 'category': category,
            'volume_9l': clean_value(row[1]),
            'volume_pct_change': clean_value(row[2]),
            'dollar_sales': clean_value(row[3]),
            'dollar_pct_change': clean_value(row[4]),
            'price_mix': None
        }
        rolling = {
            'year': year, 'month': month, 'report_date': report_date,
            'report_type': 'rolling_12', 'table_source': table_source,
            'channel': channel, 'product_type': product_type,
            'geography_type': geography_type, 'category': category,
            'volume_9l': clean_value(row[5]),
            'volume_pct_change': clean_value(row[6]),
            'dollar_sales': clean_value(row[7]),
            'dollar_pct_change': clean_value(row[8]),
            'price_mix': clean_value(row[9]) if len(row) > 9 else None
        }
    else:
        monthly = {
            'year': year, 'month': month, 'report_date': report_date,
            'report_type': 'monthly', 'table_source': table_source,
            'channel': channel, 'product_type': product_type,
            'geography_type': geography_type, 'state_name': state_name,
            'volume_9l': clean_value(row[1]),
            'volume_pct_change': clean_value(row[2]),
            'dollar_sales': clean_value(row[3]),
            'dollar_pct_change': clean_value(row[4])
        }
        rolling = {
            'year': year, 'month': month, 'report_date': report_date,
            'report_type': 'rolling_12', 'table_source': table_source,
            'channel': channel, 'product_type': product_type,
            'geography_type': geography_type, 'state_name': state_name,
            'volume_9l': clean_value(row[5]),
            'volume_pct_change': clean_value(row[6]),
            'dollar_sales': clean_value(row[7]),
            'dollar_pct_change': clean_value(row[8])
        }

    records.append(monthly)
    records.append(rolling)
    return records


def extract_commentary(text_lines):
    """Extract narrative text from PDF, filtering out headers/tables/numbers."""
    filtered = []
    for line in text_lines:
        line = line.strip()
        if not line:
            continue
        if re.search(r'^NABCA Monthly Report|Control States Results$|^\d+$|^Page \d+', line, re.I):
            continue
        if re.search(r'\d+,\d+.*\d+,\d+.*\d+,\d+', line):
            continue
        if len(line) < 50 and re.search(r'(9L|CMTY|R12TY|Shelf \$)', line):
            continue
        state_names = list(STATE_CODES.keys())
        if len(line) < 40 and any(state in line for state in state_names):
            continue
        if re.search(r'^[0-9$%\s,\-\.]+$', line):
            continue
        if len(line) > 40:
            filtered.append(line)
    return ' '.join(filtered) if filtered else ''


# ============================================================
# Process a single PDF
# ============================================================

def process_pdf(textract_client, s3_key):
    """Extract all data from one PDF. Returns dict with sales/brands/commentary or None."""
    filename = os.path.basename(s3_key)
    year, month = parse_filename(filename)
    if not year or not month:
        print(f"    WARNING: Could not parse year/month from '{filename}', skipping")
        return None

    report_date = f"{year}-{month:02d}-01"
    print(f"  Processing: {filename} ({year}-{month:02d})")

    # Run Textract
    job_id = start_textract(textract_client, s3_key)
    if not job_id:
        return None

    print(f"    Waiting for Textract...", end='', flush=True)
    success, blocks = get_textract_results(textract_client, job_id)
    if not success:
        print(f" FAILED: {blocks}")
        return None
    print(" Done")

    # Parse tables
    tables = extract_tables_from_blocks(blocks)
    text_lines = extract_text_lines(blocks)
    print(f"    Found {len(tables)} tables")

    # Identify table types
    table_types = [identify_table_type(t, i) for i, t in enumerate(tables)]
    spirits_markets_indices = [i for i, t in enumerate(table_types) if 'spirits_markets_' in str(t)]
    if len(spirits_markets_indices) >= 2:
        table_types[spirits_markets_indices[0]] = 'spirits_markets_total'
        table_types[spirits_markets_indices[-1]] = 'spirits_markets_on_premise'

    # Build records
    sales_records = []
    brand_records = []

    for table, ttype in zip(tables, table_types):
        if ttype == 'spirits_categories':
            for row in table[1:]:
                brand_records.extend(
                    parse_table_row(row, year, month, report_date,
                                    'spirits_categories', 'total', 'spirits'))
        elif ttype in ['spirits_markets_total', 'wine', 'spirits_markets_on_premise']:
            channel = 'on_premise' if ttype == 'spirits_markets_on_premise' else 'total'
            product = 'wine' if ttype == 'wine' else 'spirits'
            source = ('spirits_on_premise' if ttype == 'spirits_markets_on_premise'
                      else ('wine' if ttype == 'wine' else 'spirits_markets'))
            for row in table[1:]:
                sales_records.extend(
                    parse_table_row(row, year, month, report_date, source, channel, product))

    # Commentary
    commentary_text = extract_commentary(text_lines)
    commentary_records = []
    if commentary_text and len(commentary_text) > 50:
        commentary_records.append({
            'commentary_id': f"NABCA-{year}-{month:02d}-001",
            'year': year, 'month': month,
            'report_date': report_date,
            'section': 'full_report',
            'content': commentary_text
        })

    print(f"    Extracted: {len(sales_records)} sales, {len(brand_records)} brand, "
          f"{len(commentary_records)} commentary")

    return {
        'year': year, 'month': month,
        'sales': sales_records,
        'brands': brand_records,
        'commentary': commentary_records
    }


# ============================================================
# Supabase Upload
# ============================================================

def init_supabase():
    """Initialize Supabase client. Validates credentials are set."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("ERROR: Supabase credentials not set.")
        print("  Set SUPABASE_URL and SUPABASE_KEY environment variables.")
        sys.exit(1)

    from supabase import create_client
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def delete_existing_month(supabase_client, table_name, year, month):
    """Delete existing records for a year/month before re-inserting (prevents duplicates)."""
    try:
        (supabase_client
         .schema(SUPABASE_SCHEMA)
         .table(table_name)
         .delete()
         .eq('year', year)
         .eq('month', month)
         .execute())
        return True
    except Exception as e:
        print(f"      WARNING: Could not delete existing {table_name} data for {year}-{month:02d}: {e}")
        return False


def upload_batch(supabase_client, table_name, data, batch_size=100):
    """Upload records in batches. Returns (uploaded_count, failed_count)."""
    if not data:
        return 0, 0

    uploaded = 0
    failed = 0
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        try:
            supabase_client.schema(SUPABASE_SCHEMA).table(table_name).insert(batch).execute()
            uploaded += len(batch)
        except Exception as e:
            failed += len(batch)
            print(f"      ERROR uploading batch to {table_name}: {e}")
    return uploaded, failed


def upload_results(supabase_client, all_results):
    """Upload all extracted data to Supabase, deleting existing months first."""
    # Collect unique months being processed
    months_processed = set()
    for result in all_results:
        months_processed.add((result['year'], result['month']))

    # Delete existing data for those months (prevents duplicates on re-run)
    print("\n  Clearing existing data for processed months...")
    for year, month in sorted(months_processed):
        print(f"    Clearing {year}-{month:02d}...")
        delete_existing_month(supabase_client, 'sales_fact', year, month)
        delete_existing_month(supabase_client, 'brand_category_data', year, month)
        delete_existing_month(supabase_client, 'commentary', year, month)

    # Aggregate all records
    all_sales = [rec for r in all_results for rec in r['sales']]
    all_brands = [rec for r in all_results for rec in r['brands']]
    all_commentary = [rec for r in all_results for rec in r['commentary']]

    # Upload
    print(f"\n  Uploading {len(all_sales)} sales records...")
    s_up, s_fail = upload_batch(supabase_client, 'sales_fact', all_sales)
    print(f"    {s_up} uploaded, {s_fail} failed")

    print(f"  Uploading {len(all_brands)} brand/category records...")
    b_up, b_fail = upload_batch(supabase_client, 'brand_category_data', all_brands)
    print(f"    {b_up} uploaded, {b_fail} failed")

    print(f"  Uploading {len(all_commentary)} commentary records...")
    c_up, c_fail = upload_batch(supabase_client, 'commentary', all_commentary, batch_size=50)
    print(f"    {c_up} uploaded, {c_fail} failed")

    total_up = s_up + b_up + c_up
    total_fail = s_fail + b_fail + c_fail
    return total_up, total_fail


# ============================================================
# Main
# ============================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description='NABCA Control States Pipeline -- extract PDF data and upload to Supabase'
    )
    parser.add_argument(
        'months', nargs='*',
        help='Month(s) to process as YYYY-MM (e.g., 2025-12). If omitted, processes all PDFs in S3.'
    )
    parser.add_argument(
        '--no-upload', action='store_true',
        help='Extract only -- save JSON files but do not upload to Supabase'
    )
    parser.add_argument(
        '--output-dir', default='.',
        help='Directory for JSON output files (default: current directory)'
    )
    return parser.parse_args()


def main():
    args = parse_args()

    print("=" * 65)
    print("  NABCA Control States Pipeline")
    print("=" * 65)

    # Parse target months
    target_months = None
    if args.months:
        target_months = set()
        for m in args.months:
            try:
                parts = m.split('-')
                year, month = int(parts[0]), int(parts[1])
                target_months.add((year, month))
            except (ValueError, IndexError):
                print(f"  ERROR: Invalid month format '{m}'. Use YYYY-MM (e.g., 2025-12)")
                sys.exit(1)
        print(f"  Target months: {', '.join(f'{y}-{m:02d}' for y, m in sorted(target_months))}")
    else:
        print("  Target: ALL PDFs in S3")

    # Init AWS
    textract_client, s3_client = init_aws_clients()

    # List PDFs
    print(f"\n  S3 bucket: {S3_BUCKET}/{S3_PREFIX}")
    pdf_keys = list_s3_pdfs(s3_client, target_months)

    if not pdf_keys:
        if target_months:
            print(f"  No PDFs found matching requested months.")
            print(f"  Available PDFs in s3://{S3_BUCKET}/{S3_PREFIX}:")
            all_pdfs = list_s3_pdfs(s3_client, None)
            for key in all_pdfs:
                fname = os.path.basename(key)
                y, m = parse_filename(fname)
                if y:
                    print(f"    {fname} -> {y}-{m:02d}")
                else:
                    print(f"    {fname} -> (could not parse date)")
        else:
            print("  No PDFs found in S3 bucket.")
        sys.exit(1)

    print(f"  Found {len(pdf_keys)} PDF(s) to process\n")

    # Process each PDF
    all_results = []
    successful = 0
    failed = 0

    for i, s3_key in enumerate(pdf_keys, 1):
        print(f"  [{i}/{len(pdf_keys)}] {os.path.basename(s3_key)}")
        result = process_pdf(textract_client, s3_key)
        if result:
            all_results.append(result)
            successful += 1
        else:
            failed += 1

    print(f"\n  Extraction complete: {successful} succeeded, {failed} failed")

    if not all_results:
        print("  No data extracted. Exiting.")
        sys.exit(1)

    # Aggregate counts
    total_sales = sum(len(r['sales']) for r in all_results)
    total_brands = sum(len(r['brands']) for r in all_results)
    total_commentary = sum(len(r['commentary']) for r in all_results)

    print(f"  Total records: {total_sales} sales, {total_brands} brand, {total_commentary} commentary")

    # Save JSON files
    output_dir = args.output_dir
    os.makedirs(output_dir, exist_ok=True)

    all_sales = [rec for r in all_results for rec in r['sales']]
    all_brands = [rec for r in all_results for rec in r['brands']]
    all_commentary = [rec for r in all_results for rec in r['commentary']]

    sales_path = os.path.join(output_dir, 'sales_fact.json')
    brands_path = os.path.join(output_dir, 'brand_category_data.json')
    commentary_path = os.path.join(output_dir, 'commentary.json')

    with open(sales_path, 'w') as f:
        json.dump(all_sales, f, indent=2)
    print(f"\n  Saved {sales_path} ({len(all_sales)} records)")

    with open(brands_path, 'w') as f:
        json.dump(all_brands, f, indent=2)
    print(f"  Saved {brands_path} ({len(all_brands)} records)")

    with open(commentary_path, 'w') as f:
        json.dump(all_commentary, f, indent=2)
    print(f"  Saved {commentary_path} ({len(all_commentary)} records)")

    # Upload to Supabase
    if args.no_upload:
        print("\n  --no-upload flag set. Skipping Supabase upload.")
    else:
        print("\n  Uploading to Supabase...")
        print(f"  URL: {SUPABASE_URL}")
        print(f"  Schema: {SUPABASE_SCHEMA}")

        supabase_client = init_supabase()
        total_up, total_fail = upload_results(supabase_client, all_results)

        print(f"\n  Upload complete: {total_up} uploaded, {total_fail} failed")

    # Summary
    print(f"\n{'=' * 65}")
    print(f"  Pipeline complete")
    months_str = ', '.join(f"{r['year']}-{r['month']:02d}" for r in all_results)
    print(f"  Months processed: {months_str}")
    print(f"  Records: {total_sales} sales | {total_brands} brand | {total_commentary} commentary")
    print(f"{'=' * 65}")


if __name__ == '__main__':
    main()
