#!/usr/bin/env python3
"""
processing_status_report.py

Interactive console tool that:
  1. Lets the user pick one or more databases (abhi_mask = prod, abhi_maskv2..v6 = historical)
  2. Lets the user enter an upload-date range per database
  3. Runs an OPTIMIZED single-pass version of the status query
  4. Prints a pivoted table (Stage x Database), similar to the "Processing Status" report

-------------------------------------------------------------------------------
WHY THE ORIGINAL QUERY WAS SLOW / SOMETIMES HUNG
-------------------------------------------------------------------------------
The original query used 9 separate SELECT...UNION ALL branches. For every
`extractionDetails` branch it re-ran the SAME correlated subquery:

    fileId IN (SELECT id FROM files WHERE uploaded_at >= @Start AND uploaded_at < @End)

That means, per execution:
  - `files`            scanned ~6 times (3 direct branches + 3 correlated subqueries)
  - `extractionDetails` scanned 3 times
  - `documents`         scanned 2 times

Under load, or without a good index on uploaded_at/UploadDate, this multiplies
lock/IO contention and can make one "simple" report block behind other
sessions -- with no query timeout, it just hangs.

This script fixes that by:
  - Loading `files` and `documents` ONCE into indexed temp tables for the date
    range, then computing every derived stage count from those temp tables
    (single scan per source table, single join to extractionDetails).
  - Setting an explicit LOCK_TIMEOUT and a pyodbc query timeout, so a blocked
    query fails fast and is retried with backoff instead of hanging forever.
  - Logging retries/timeouts so you can tell "it's blocked" apart from
    "it's just slow."

The actual stage DEFINITIONS (which status values count as which stage) are
UNCHANGED from your original query -- only the execution plan is optimized.

-------------------------------------------------------------------------------
RECOMMENDED INDEXES (ask your DBA to add these if not present -- they matter
more than anything in this script for actually fixing the slowness):
-------------------------------------------------------------------------------
    CREATE NONCLUSTERED INDEX IX_files_uploaded_at
        ON dbo.files (uploaded_at) INCLUDE (id, processing_status, Upload_Status);

    CREATE NONCLUSTERED INDEX IX_documents_uploaddate
        ON dbo.documents (UploadDate) INCLUDE (DownloadStatus);

    CREATE NONCLUSTERED INDEX IX_extractionDetails_fileId
        ON dbo.extractionDetails (fileId)
        INCLUDE (identificationStatus, maskingStatus, processingStatus, outputFilePrepration);
-------------------------------------------------------------------------------

Requirements:
    pip install pyodbc
"""

import sys
import time
import logging
from datetime import datetime

try:
    import pyodbc
except ImportError:
    print("Missing dependency: pip install pyodbc")
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("processing_status_report")

# ------------------------------------------------------------------------- #
# CONFIG -- edit connection details per database here.
# If all 6 databases live on the SAME SQL Server instance, just change
# "database" per entry and reuse the host/creds. If historical servers are
# genuinely different hosts, fill in "server"/"username"/"password" per entry.
# ------------------------------------------------------------------------- #

DEFAULT_SERVER = "10.21.42.17,1433"
DEFAULT_USERNAME = "ABHIMASK"
# Same password used across all 6 databases (per-DB override still possible
# below via each entry's "password" key, e.g. if one historical server
# eventually gets rotated separately).
DEFAULT_PASSWORD = "abhiM@4312"

DATABASES = {
    "abhi_mask": {
        "label": "abhi_mask (PROD)",
        "server": DEFAULT_SERVER,
        "database": "abhi_mask",
        "username": DEFAULT_USERNAME,
        "password": DEFAULT_PASSWORD,
    },
    "abhi_maskv2": {
        "label": "abhi_maskv2 (Historical)",
        "server": DEFAULT_SERVER,
        "database": "abhi_maskv2",
        "username": DEFAULT_USERNAME,
        "password": DEFAULT_PASSWORD,
    },
    "abhi_maskv3": {
        "label": "abhi_maskv3 (Historical)",
        "server": DEFAULT_SERVER,
        "database": "abhi_maskv3",
        "username": DEFAULT_USERNAME,
        "password": DEFAULT_PASSWORD,
    },
    "abhi_maskv4": {
        "label": "abhi_maskv4 (Historical)",
        "server": DEFAULT_SERVER,
        "database": "abhi_maskv4",
        "username": DEFAULT_USERNAME,
        "password": DEFAULT_PASSWORD,
    },
    "abhi_maskv5": {
        "label": "abhi_maskv5 (Historical)",
        "server": DEFAULT_SERVER,
        "database": "abhi_maskv5",
        "username": DEFAULT_USERNAME,
        "password": DEFAULT_PASSWORD,
    },
    "abhi_maskv6": {
        "label": "abhi_maskv6 (Historical)",
        "server": DEFAULT_SERVER,
        "database": "abhi_maskv6",
        "username": DEFAULT_USERNAME,
        "password": DEFAULT_PASSWORD,
    },
}

QUERY_TIMEOUT_SECONDS = 60      # per-attempt query timeout
LOCK_TIMEOUT_MS = 15000         # fail fast on blocking locks instead of hanging
CONNECT_TIMEOUT_SECONDS = 15
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 5       # multiplied by attempt number

# ------------------------------------------------------------------------- #
# Optimized SQL -- single pass per source table via temp tables.
# Returns TWO result sets:
#   1) documents/files-derived counts
#   2) extractionDetails-derived counts
# ------------------------------------------------------------------------- #
OPTIMIZED_SQL = """
SET NOCOUNT ON;
SET LOCK_TIMEOUT {lock_timeout_ms};

DECLARE @StartDate DATE = ?;
DECLARE @EndDate   DATE = ?;

IF OBJECT_ID('tempdb..#Files') IS NOT NULL DROP TABLE #Files;
IF OBJECT_ID('tempdb..#Docs')  IS NOT NULL DROP TABLE #Docs;

SELECT id, processing_status, Upload_Status
INTO #Files
FROM dbo.files
WHERE uploaded_at >= @StartDate AND uploaded_at < @EndDate;

CREATE UNIQUE CLUSTERED INDEX IX_tmp_files_id ON #Files(id);

SELECT DownloadStatus
INTO #Docs
FROM dbo.documents
WHERE UploadDate >= @StartDate AND UploadDate < @EndDate;

-- Result set 1: documents + files derived stages (single scan each)
SELECT
    (SELECT COUNT(*) FROM #Docs)                                             AS TotalRecords,
    (SELECT COUNT(*) FROM #Docs  WHERE DownloadStatus = 'Downloaded')        AS Downloaded,
    (SELECT COUNT(*) FROM #Docs  WHERE DownloadStatus = 'Yet to Download')   AS DownloadPending,
    (SELECT COUNT(*) FROM #Files WHERE processing_status = 'Queued')         AS ExtractionCompleted,
    (SELECT COUNT(*) FROM #Files WHERE processing_status = 'Completed')      AS ZipCreationCompleted,
    (SELECT COUNT(*) FROM #Files WHERE Upload_Status = 'Completed')          AS UploadCompleted;

-- Result set 2: extractionDetails derived stages (single join, single scan)
SELECT
    SUM(CASE WHEN ed.identificationStatus IN ('Completed','Failed') THEN 1 ELSE 0 END) AS IdentificationCompleted,
    SUM(CASE WHEN ed.maskingStatus = 'Aadhar found' THEN 1 ELSE 0 END)                 AS AadhaarMaskedCount,
    SUM(CASE WHEN ed.processingStatus = 'Completed' THEN 1 ELSE 0 END)                 AS MiddlewareCompleted,
    SUM(CASE WHEN ed.outputFilePrepration = 'Completed' THEN 1 ELSE 0 END)             AS OutputCreationCompleted
FROM #Files f
JOIN dbo.extractionDetails ed ON ed.fileId = f.id;

DROP TABLE #Files;
DROP TABLE #Docs;
"""

STAGE_ORDER = [
    ("Total Records", "TotalRecords"),
    ("Download Pending", "DownloadPending"),
    ("Downloaded", "Downloaded"),
    ("Extraction Completed", "ExtractionCompleted"),
    ("Identification Completed", "IdentificationCompleted"),
    ("Aadhaar Masked Count", "AadhaarMaskedCount"),
    ("Output Creation Completed", "OutputCreationCompleted"),
    ("Zip Creation Completed", "ZipCreationCompleted"),
    ("Upload Completed", "UploadCompleted"),
]


def get_connection(db_key: str):
    """Open a connection with a bounded login timeout."""
    cfg = DATABASES[db_key]
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={cfg['server']};"
        f"DATABASE={cfg['database']};"
        f"UID={cfg['username']};"
        f"PWD={cfg['password']};"
        "TrustServerCertificate=yes;"
        "Encrypt=yes;"
    )
    conn = pyodbc.connect(conn_str, timeout=CONNECT_TIMEOUT_SECONDS)
    return conn


def run_status_query(db_key: str, start_date: str, end_date: str) -> dict:
    """
    Executes the optimized query with retry + backoff on timeout/blocking.
    Returns a dict of {field_name: count}.
    """
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        conn = None
        try:
            t0 = time.time()
            conn = get_connection(db_key)
            cursor = conn.cursor()
            cursor.timeout = QUERY_TIMEOUT_SECONDS  # pyodbc query timeout (seconds)

            sql = OPTIMIZED_SQL.format(lock_timeout_ms=LOCK_TIMEOUT_MS)
            cursor.execute(sql, start_date, end_date)

            row1 = cursor.fetchone()
            cols1 = [c[0] for c in cursor.description]
            result = dict(zip(cols1, row1)) if row1 else {}

            if cursor.nextset():
                row2 = cursor.fetchone()
                cols2 = [c[0] for c in cursor.description]
                result.update(dict(zip(cols2, row2)) if row2 else {})

            elapsed = time.time() - t0
            log.info(f"[{db_key}] query completed in {elapsed:.2f}s (attempt {attempt})")
            return result

        except pyodbc.Error as e:
            last_err = e
            log.warning(f"[{db_key}] attempt {attempt}/{MAX_RETRIES} failed: {e}")
            if attempt < MAX_RETRIES:
                sleep_for = RETRY_BACKOFF_SECONDS * attempt
                log.info(f"[{db_key}] retrying in {sleep_for}s "
                         f"(likely blocking lock or timeout, not a script bug)")
                time.sleep(sleep_for)
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    log.error(f"[{db_key}] gave up after {MAX_RETRIES} attempts: {last_err}")
    return {}


# ------------------------------------------------------------------------- #
# Console interaction
# ------------------------------------------------------------------------- #

def prompt_database_selection() -> list:
    print("\nAvailable databases:")
    keys = list(DATABASES.keys())
    for i, k in enumerate(keys, start=1):
        print(f"  {i}. {DATABASES[k]['label']}")
    print(f"  {len(keys)+1}. ALL")

    raw = input("\nSelect database(s) by number (comma-separated, or 'all'): ").strip().lower()
    if raw in ("all", str(len(keys) + 1)):
        return keys

    selected = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        if not part.isdigit() or not (1 <= int(part) <= len(keys)):
            print(f"Ignoring invalid selection: '{part}'")
            continue
        selected.append(keys[int(part) - 1])

    if not selected:
        print("No valid database selected. Exiting.")
        sys.exit(1)
    return selected


def prompt_date(label: str) -> str:
    while True:
        raw = input(f"  {label} (YYYY-MM-DD): ").strip()
        try:
            datetime.strptime(raw, "%Y-%m-%d")
            return raw
        except ValueError:
            print("  Invalid date format, try again (e.g. 2026-05-01).")


def prompt_date_range_for(db_key: str) -> tuple:
    print(f"\nUpload date range for {DATABASES[db_key]['label']}:")
    start_date = prompt_date("Start date (inclusive)")
    end_date = prompt_date("End date (exclusive)")
    return start_date, end_date


def print_grid_table(headers: list, rows: list):
    """Minimal dependency-free ASCII grid table (no tabulate needed)."""
    all_rows = [headers] + rows
    col_widths = [
        max(len(str(row[i])) for row in all_rows)
        for i in range(len(headers))
    ]

    def sep_line():
        return "+" + "+".join("-" * (w + 2) for w in col_widths) + "+"

    def fmt_row(row):
        cells = [str(cell).ljust(col_widths[i]) for i, cell in enumerate(row)]
        return "| " + " | ".join(cells) + " |"

    print(sep_line())
    print(fmt_row(headers))
    print(sep_line())
    for row in rows:
        print(fmt_row(row))
    print(sep_line())


def main():
    print("=" * 70)
    print(" Processing Status Report")
    print("=" * 70)

    selected_dbs = prompt_database_selection()

    same_range = "y"
    if len(selected_dbs) > 1:
        same_range = input(
            "\nUse the SAME upload-date range for all selected databases? (y/n): "
        ).strip().lower() or "y"

    date_ranges = {}
    if same_range == "y":
        start_date, end_date = prompt_date_range_for(selected_dbs[0])
        for k in selected_dbs:
            date_ranges[k] = (start_date, end_date)
    else:
        for k in selected_dbs:
            date_ranges[k] = prompt_date_range_for(k)

    results = {}
    for k in selected_dbs:
        start_date, end_date = date_ranges[k]
        print(f"\nRunning query on {DATABASES[k]['label']} "
              f"[{start_date} -> {end_date}] ...")
        results[k] = run_status_query(k, start_date, end_date)

    # Build pivoted table: rows = stage, columns = database
    headers = ["Stage"] + [DATABASES[k]["label"] for k in selected_dbs]
    table_rows = []
    for stage_label, field in STAGE_ORDER:
        row = [stage_label]
        for k in selected_dbs:
            val = results.get(k, {}).get(field)
            row.append(val if val is not None else "N/A")
        table_rows.append(row)

    print("\n" + "=" * 70)
    print(f" Processing Status ({datetime.now().strftime('%d-%b-%Y')})")
    print("=" * 70)
    print_grid_table(headers, table_rows)


if __name__ == "__main__":
    main()
