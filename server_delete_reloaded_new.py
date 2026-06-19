import os
import shutil
import traceback
from collections import defaultdict
from datetime import datetime
import pyodbc

# ─────────────────────────────────────────────
#  CONNECTION CONSTANTS
# ─────────────────────────────────────────────
SERVER   = '10.21.42.17,1433'
DATABASE = 'abhi_mask'
USERNAME = 'ABHIMASK'
PASSWORD = 'abhiM@4312'

DOCUMENTS_TABLE          = "dbo.documents"
FILES_TABLE              = "dbo.files"
EXTRACTION_DETAILS_TABLE = "dbo.extractionDetails"

# ─────────────────────────────────────────────
#  OPERATIONAL CONSTANTS
# ─────────────────────────────────────────────
FREE_SPACE_THRESHOLD_GB = 200
DATA_ROOT_PATH          = "/data"

# Each entry: (label, start_date_inclusive, end_date_exclusive)
# Date-range filtering avoids YEAR()/MONTH() table scans and leverages
# any index on documents.UploadDate.
TARGET_MONTHS = [
    (
        "Jan-2026",
        "2026-01-01 00:00:00.000",
        "2026-02-01 00:00:00.000",
    ),
    (
        "Feb-2026",
        "2026-02-01 00:00:00.000",
        "2026-03-01 00:00:00.000",
    ),
]


# ═══════════════════════════════════════════════════════════════
#  DISK UTILITIES
# ═══════════════════════════════════════════════════════════════

def bytes_to_gb(num_bytes):
    """Convert a byte count to GB (base-1024)."""
    return num_bytes / (1024 ** 3)


def get_disk_usage(path):
    """
    Returns disk usage statistics (in GB + free%) for the given path.
    Keys: total_gb, used_gb, free_gb, free_pct
    """
    usage = shutil.disk_usage(path)
    total = usage.total or 1          # guard against division by zero
    return {
        "total_gb": bytes_to_gb(usage.total),
        "used_gb":  bytes_to_gb(usage.used),
        "free_gb":  bytes_to_gb(usage.free),
        "free_pct": (usage.free / total) * 100,
    }


def print_disk_usage(title, usage):
    """Pretty-prints a disk usage dict produced by get_disk_usage()."""
    print(f"\n{'=' * 52}")
    print(f"  {title}")
    print(f"{'=' * 52}")
    print(f"  Total Disk Space   : {usage['total_gb']:.2f} GB")
    print(f"  Used Space         : {usage['used_gb']:.2f} GB")
    print(f"  Free Space         : {usage['free_gb']:.2f} GB")
    print(f"  Free Space (%)     : {usage['free_pct']:.1f}%")
    print(f"{'=' * 52}")


# ═══════════════════════════════════════════════════════════════
#  DATABASE UTILITIES
# ═══════════════════════════════════════════════════════════════

def connect_to_db():
    """Establishes a connection to the SQL Server database."""
    try:
        driver = "{ODBC Driver 18 for SQL Server}"
        connection_string = (
            f"DRIVER={driver};"
            f"SERVER={SERVER};"
            f"DATABASE={DATABASE};"
            f"UID={USERNAME};"
            f"PWD={PASSWORD};"
            f"TrustServerCertificate=yes;"
        )
        conn = pyodbc.connect(connection_string)
        print("\n[DB] Successfully connected to the database.")
        return conn
    except pyodbc.Error as ex:
        print(f"[DB ERROR] Could not connect: {ex}")
        print("  Ensure the ODBC driver name is correct and the server is accessible.")
        return None


def _rows_to_dicts(cursor, rows):
    """Helper – convert pyodbc rows to a list of dicts."""
    if not rows:
        return []
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in rows]


def fetch_records_for_month(cursor, label, start_date, end_date):
    """
    Fetches records where:
      • ed.maskingStatus = 'Aadhar not found'  (case-insensitive, trimmed)
      • d.UploadDate falls within [start_date, end_date)

    Uses a half-open date-range predicate (>= start / < end) so that SQL
    Server can perform an index seek on documents.UploadDate instead of a
    full table scan (avoids YEAR()/MONTH() function wrapping).

    Join chain: documents → files → extractionDetails
      documents.documentindex = files.documentindex
      files.id                = extractionDetails.fileId

    Returns a list of dicts with keys:
        documentindex, upload_date, file_id, file_name,
        binaryFilePath, extractedFilePath, outputFilePath,
        pickleInputPath, pickleOutputPath
    """
    try:
        query = f"""
            SELECT
                d.documentindex,
                d.UploadDate            AS upload_date,
                f.id                    AS file_id,
                f.file_name,
                ed.binaryFilePath,
                ed.extractedFilePath,
                ed.outputFilePath,
                ed.pickleInputPath,
                ed.pickleOutputPath
            FROM {DOCUMENTS_TABLE} d
            INNER JOIN {FILES_TABLE} f
                ON d.documentindex = f.documentindex
            INNER JOIN {EXTRACTION_DETAILS_TABLE} ed
                ON ed.fileId = f.id
            WHERE LOWER(LTRIM(RTRIM(ed.maskingStatus))) = 'aadhar not found'
              AND d.UploadDate >= ?
              AND d.UploadDate <  ?
        """
        cursor.execute(query, (start_date, end_date))
        rows = cursor.fetchall()
        result = _rows_to_dicts(cursor, rows)
        print(f"  [DB] {len(result)} record(s) found for {label} "
              f"(UploadDate >= '{start_date}' AND < '{end_date}').")
        return result
    except pyodbc.Error as ex:
        print(f"  [DB ERROR] fetch_records_for_month({label}): {ex}")
        return []


# ═══════════════════════════════════════════════════════════════
#  FILE-PATH HELPERS
# ═══════════════════════════════════════════════════════════════

# Only the five extraction paths are targeted for deletion / size calc.
EXTRACTION_PATH_FIELDS = [
    ("binaryFilePath",    "binary file"),
    ("extractedFilePath", "extracted file"),
    ("outputFilePath",    "masked output file"),
    ("pickleInputPath",   "pickle input file"),
    ("pickleOutputPath",  "pickle output file"),
]


def get_extraction_paths(record):
    """Returns [(abs_path, description), …] for a single record."""
    results = []
    for field, desc in EXTRACTION_PATH_FIELDS:
        raw = record.get(field)
        if raw and raw.strip():
            results.append((os.path.abspath(os.path.normpath(raw.strip())), desc))
    return results


# ═══════════════════════════════════════════════════════════════
#  STORAGE CALCULATION
# ═══════════════════════════════════════════════════════════════

def calculate_storage_for_records(records):
    """
    Scans all extraction file paths in `records`, de-duplicating physical
    paths so the same file is never counted twice.

    Returns: (unique_existing_paths: set, total_bytes: int, missing_count: int)
    """
    seen_paths   = set()
    total_bytes  = 0
    missing_count = 0

    for record in records:
        for abs_path, _ in get_extraction_paths(record):
            if abs_path in seen_paths:
                continue
            seen_paths.add(abs_path)

            if os.path.isfile(abs_path):
                try:
                    total_bytes += os.path.getsize(abs_path)
                except OSError:
                    pass
            else:
                missing_count += 1

    existing_paths = {p for p in seen_paths if os.path.isfile(p)}
    return existing_paths, total_bytes, missing_count


# ═══════════════════════════════════════════════════════════════
#  REPORTING
# ═══════════════════════════════════════════════════════════════

def print_storage_summary(month_data):
    """
    Prints the month-wise and overall storage summary.

    `month_data` is a list of dicts, each with keys:
        label, record_count, existing_paths, total_bytes, missing_count
    """
    print("\n" + "=" * 60)
    print("  AADHAAR NOT FOUND – STORAGE UTILIZATION SUMMARY")
    print("=" * 60)

    grand_records  = 0
    grand_bytes    = 0
    grand_files    = 0
    grand_missing  = 0

    for md in month_data:
        gb = bytes_to_gb(md["total_bytes"])
        print(f"\n  Month            : {md['label']}")
        print(f"  Record Count     : {md['record_count']}")
        print(f"  Existing Files   : {len(md['existing_paths'])}")
        print(f"  Missing Files    : {md['missing_count']}")
        print(f"  Storage Occupied : {gb:.4f} GB")
        print(f"  {'-' * 46}")

        grand_records += md["record_count"]
        grand_bytes   += md["total_bytes"]
        grand_files   += len(md["existing_paths"])
        grand_missing += md["missing_count"]

    print(f"\n  ── OVERALL TOTAL ──────────────────────────────")
    print(f"  Total Records    : {grand_records}")
    print(f"  Total Exist.Files: {grand_files}")
    print(f"  Total Missing    : {grand_missing}")
    print(f"  Total Storage    : {bytes_to_gb(grand_bytes):.4f} GB")
    print("=" * 60)

    return grand_bytes


def print_deletion_candidates(month_data):
    """Displays month-wise deletion candidates before asking for confirmation."""
    print("\n" + "-" * 60)
    print("  DELETION CANDIDATES")
    print("-" * 60)
    for md in month_data:
        gb = bytes_to_gb(md["total_bytes"])
        print(f"  {md['label']:<12}  |  {md['record_count']:>6} records  "
              f"|  {gb:.4f} GB reclaimable")
    print("-" * 60)


# ═══════════════════════════════════════════════════════════════
#  FILE DELETION
# ═══════════════════════════════════════════════════════════════

def delete_file_from_system(abs_path, description="file"):
    """
    Deletes a single file.  abs_path must already be an absolute path.

    Returns: (success: bool, status: str, size_bytes: int)
    status ∈ {"deleted", "not_found", "not_a_file", "error"}
    """
    if os.path.exists(abs_path):
        if os.path.isfile(abs_path):
            try:
                size_bytes = os.path.getsize(abs_path)
                os.remove(abs_path)
                print(f"    [DELETED] {description}: {abs_path}  "
                      f"({bytes_to_gb(size_bytes):.6f} GB)")
                return True, "deleted", size_bytes
            except OSError as e:
                print(f"    [ERROR] Cannot delete {abs_path}: {e}")
                return False, "error", 0
        else:
            print(f"    [SKIP] Not a file (directory?): {abs_path}")
            return False, "not_a_file", 0
    else:
        print(f"    [NOT FOUND] {abs_path}")
        return False, "not_found", 0


def perform_cleanup(all_records):
    """
    Deletes every extraction file referenced across all_records,
    de-duplicating so the same physical path is never touched twice.

    Returns a stats dict.
    """
    stats = {
        "deleted":       0,
        "not_found":     0,
        "errors":        0,
        "deleted_bytes": 0,
    }
    deleted_paths = set()

    print(f"\n--- Starting Deletion: {len(all_records)} eligible record(s) ---")

    for record in all_records:
        file_id   = record.get("file_id")
        file_name = record.get("file_name")
        print(f"\n  FileID: {file_id}  |  {file_name}")

        for abs_path, description in get_extraction_paths(record):
            if abs_path in deleted_paths:
                continue   # already deleted in this run

            success, status, size_bytes = delete_file_from_system(abs_path, description)

            if success:
                stats["deleted"]       += 1
                stats["deleted_bytes"] += size_bytes
                deleted_paths.add(abs_path)
            elif status == "not_found":
                stats["not_found"] += 1
            elif status == "error":
                stats["errors"] += 1
            # "not_a_file" – skip silently (already printed)

    return stats


# ═══════════════════════════════════════════════════════════════
#  USER CONFIRMATION
# ═══════════════════════════════════════════════════════════════

def prompt_user_confirmation(month_labels):
    """
    Asks the user whether to delete files for the listed months.
    Returns True only on explicit 'yes'.
    """
    label_str = " and ".join(month_labels)
    print(f"\nDo you want to delete Aadhaar Not Found files for {label_str}? (yes/no): ",
          end="", flush=True)
    while True:
        try:
            answer = input().strip().lower()
        except (EOFError, KeyboardInterrupt):
            print("\nInput interrupted. Exiting without deleting.")
            return False

        if answer == "yes":
            return True
        elif answer == "no":
            return False
        else:
            print("  Invalid input – please type 'yes' or 'no': ", end="", flush=True)


# ═══════════════════════════════════════════════════════════════
#  FINAL REPORT
# ═══════════════════════════════════════════════════════════════

def print_final_report(records_processed, stats, disk_before, disk_after):
    """Prints the post-cleanup summary."""
    deleted_gb = bytes_to_gb(stats["deleted_bytes"])

    print("\n" + "=" * 52)
    print("  CLEANUP COMPLETED")
    print("=" * 52)
    print(f"  Records Processed    : {records_processed}")
    print(f"  Files Deleted        : {stats['deleted']}")
    print(f"  Files Not Found      : {stats['not_found']}")
    print(f"  Deletion Failures    : {stats['errors']}")
    print(f"  Space Reclaimed      : {deleted_gb:.4f} GB")
    print(f"\n  Free Space Before    : {disk_before['free_gb']:.2f} GB  "
          f"({disk_before['free_pct']:.1f}%)")
    print(f"  Free Space After     : {disk_after['free_gb']:.2f} GB  "
          f"({disk_after['free_pct']:.1f}%)")
    print(f"\n  Total Disk Size      : {disk_after['total_gb']:.2f} GB")
    print(f"  Used Space After     : {disk_after['used_gb']:.2f} GB")
    print("=" * 52)


# ═══════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════

def main():
    """Main orchestration for the Aadhaar storage cleanup process."""

    # ── Step 1 : Disk usage snapshot ───────────────────────────
    print("\n[INFO] Gathering disk usage information...")
    disk_before = get_disk_usage(DATA_ROOT_PATH)
    print_disk_usage(f"Disk Usage for '{DATA_ROOT_PATH}'", disk_before)

    # ── Step 2 : Connect to DB ─────────────────────────────────
    conn = connect_to_db()
    if not conn:
        return

    cursor = None
    try:
        cursor = conn.cursor()

        # ── Step 3 : Fetch records month-wise ──────────────────
        print("\n[INFO] Querying database for Aadhaar Not Found records "
              "(Jan-2026 & Feb-2026)...")

        month_data = []
        all_combined_records = []

        for label, start_date, end_date in TARGET_MONTHS:
            records = fetch_records_for_month(cursor, label, start_date, end_date)
            existing_paths, total_bytes, missing_count = calculate_storage_for_records(records)
            month_data.append({
                "label":          label,
                "start_date":     start_date,
                "end_date":       end_date,
                "records":        records,
                "record_count":   len(records),
                "existing_paths": existing_paths,
                "total_bytes":    total_bytes,
                "missing_count":  missing_count,
            })
            all_combined_records.extend(records)

        # ── Step 4 : Storage utilization report ────────────────
        print_storage_summary(month_data)

        # ── Step 5 : Free-space threshold check ────────────────
        if disk_before["free_gb"] >= FREE_SPACE_THRESHOLD_GB:
            print(f"\n[OK] Free space ({disk_before['free_gb']:.2f} GB) is above the "
                  f"threshold ({FREE_SPACE_THRESHOLD_GB} GB).")
            print("Free space is above threshold. No cleanup required.")
            return

        # ── Step 6 : Below threshold – cleanup path ────────────
        print(f"\n[WARNING] Free space ({disk_before['free_gb']:.2f} GB) is below the "
              f"threshold ({FREE_SPACE_THRESHOLD_GB} GB). Cleanup is recommended.")

        if not all_combined_records:
            print("\n[INFO] No eligible records found for cleanup. Exiting.")
            return

        print_deletion_candidates(month_data)

        # ── Step 7 : User confirmation ─────────────────────────
        month_labels = [md["label"] for md in month_data]
        if not prompt_user_confirmation(month_labels):
            print("\n[INFO] Deletion cancelled by user. No files were deleted.")
            return

        # ── Step 8 : Perform deletion ──────────────────────────
        stats = perform_cleanup(all_combined_records)

        # ── Step 9 : Final report ──────────────────────────────
        disk_after = get_disk_usage(DATA_ROOT_PATH)
        print_final_report(
            records_processed=len(all_combined_records),
            stats=stats,
            disk_before=disk_before,
            disk_after=disk_after,
        )

    except Exception as e:
        print(f"\n[FATAL] Unexpected error: {e}")
        print("Traceback:")
        traceback.print_exc()
    finally:
        if cursor:
            cursor.close()
            print("\n[DB] Cursor closed.")
        if conn:
            conn.close()
            print("[DB] Connection closed.")


if __name__ == "__main__":
    main()