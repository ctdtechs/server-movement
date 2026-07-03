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
import base64
import smtplib
import ssl
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

try:
    import pyodbc
except ImportError:
    print("Missing dependency: pip install pyodbc")
    sys.exit(1)

# ------------------------------------------------------------------------- #
# Console colors (ANSI escape codes -- no extra dependency needed).
# Auto-disabled when stdout isn't a real terminal (piped/redirected), so
# nothing garbled ends up in files. The log FILE handler stays plain text
# regardless -- only the console stream gets colored.
# ------------------------------------------------------------------------- #
COLOR_ENABLED = sys.stdout.isatty()


class C:
    RESET = "\033[0m" if COLOR_ENABLED else ""
    BOLD = "\033[1m" if COLOR_ENABLED else ""
    DIM = "\033[2m" if COLOR_ENABLED else ""
    CYAN = "\033[36m" if COLOR_ENABLED else ""
    GREEN = "\033[32m" if COLOR_ENABLED else ""
    YELLOW = "\033[33m" if COLOR_ENABLED else ""
    RED = "\033[31m" if COLOR_ENABLED else ""
    MAGENTA = "\033[35m" if COLOR_ENABLED else ""
    BLUE = "\033[34m" if COLOR_ENABLED else ""
    GRAY = "\033[90m" if COLOR_ENABLED else ""


class ColorConsoleFormatter(logging.Formatter):
    LEVEL_COLORS = {
        logging.INFO: C.GREEN,
        logging.WARNING: C.YELLOW,
        logging.ERROR: C.RED,
        logging.CRITICAL: C.RED + C.BOLD,
    }

    def format(self, record):
        color = self.LEVEL_COLORS.get(record.levelno, "")
        base = super().format(record)
        return f"{color}{base}{C.RESET}"


LOG_FILE = "status_report.log"

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(ColorConsoleFormatter("%(asctime)s [%(levelname)s] %(message)s"))

file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

logging.basicConfig(level=logging.INFO, handlers=[console_handler, file_handler])
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

QUERY_TIMEOUT_SECONDS = 180     # per-attempt query timeout (headroom beyond lock timeout)
LOCK_TIMEOUT_MS = 60000         # fail fast on blocking locks instead of hanging
CONNECT_TIMEOUT_SECONDS = 15
MAX_RETRIES = 4
RETRY_BACKOFF_SECONDS = 10       # multiplied by attempt number

# ------------------------------------------------------------------------- #
# EMAIL CONFIG -- edit these.
# ------------------------------------------------------------------------- #
SMTP_SERVER = "smtp.office365.com"
SMTP_PORT = 587

MAIL_FROM = "vn@ctdtechs.com"

# Password stored base64-encoded (NOT real encryption -- just avoids a raw
# plaintext password sitting in the file at a glance; anyone with this file
# can still decode it in one line).
# Generate with: python3 -c "import base64; print(base64.b64encode(b'your-password').decode())"
MAIL_PASSWORD_B64 = "eW91ci1wYXNzd29yZC1vci1hcHAtcGFzc3dvcmQ="  # "your-password-or-app-password"

# Hardcoded defaults, semicolon-separated (e.g. "a@ctdtechs.com;b@ctdtechs.com").
# The console will also offer to ADD more recipients on top of these at runtime.
DEFAULT_MAIL_TO = "nv@ctdtechs.com"
DEFAULT_MAIL_CC = ""  # e.g. "manager@ctdtechs.com;lead@ctdtechs.com"


def get_mail_password() -> str:
    try:
        return base64.b64decode(MAIL_PASSWORD_B64).decode("utf-8")
    except Exception as e:
        raise ValueError(
            f"MAIL_PASSWORD_B64 is not valid base64: {e}. "
            "Generate it with: python3 -c \"import base64; "
            "print(base64.b64encode(b'your-password').decode())\""
        )


def parse_addr_list(raw: str) -> list:
    """Splits a semicolon-separated address string into a clean list."""
    return [a.strip() for a in raw.split(";") if a.strip()]

# ------------------------------------------------------------------------- #
# Optimized SQL -- single pass per source table via temp tables.
# Returns TWO result sets:
#   1) documents/files-derived counts
#   2) extractionDetails-derived counts
# ------------------------------------------------------------------------- #
OPTIMIZED_SQL = """
SET NOCOUNT ON;
SET LOCK_TIMEOUT {lock_timeout_ms};
-- Reporting query: don't block on / wait for locks held by concurrent
-- writers. Trades strict consistency for speed (may read uncommitted /
-- in-flight rows) -- acceptable for a status dashboard, not for anything
-- requiring exact transactional accuracy.
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

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
    ("Month", "Month"),
    ("Total Records (Unique)", "TotalRecords"),
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


# SQLSTATE prefixes worth retrying (transient): timeouts, deadlocks, dropped
# connections, general connection failures.
TRANSIENT_SQLSTATES = ("HYT00", "HYT01", "40001", "08S01", "08001", "08004")

# SQL Server sometimes reports a genuinely transient condition (lock timeout,
# deadlock victim, network blip) under a generic SQLSTATE like 42000, with the
# real reason only visible in the message text / native error number. Catch
# those here so they still get retried instead of being treated as a
# permission/syntax error.
TRANSIENT_MESSAGE_MARKERS = (
    "lock request time out period exceeded",  # native error 1222
    "deadlock",                               # native error 1205
    "timeout expired",
    "communication link failure",
    "general network error",
    "transport-level error",
    "connection is busy",
)


def is_transient_error(exc: pyodbc.Error) -> bool:
    sqlstate = str(exc.args[0]).upper() if exc.args else ""
    if sqlstate in TRANSIENT_SQLSTATES:
        return True
    message = str(exc).lower()
    return any(marker in message for marker in TRANSIENT_MESSAGE_MARKERS)


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
            conn.timeout = QUERY_TIMEOUT_SECONDS  # pyodbc query timeout lives on the connection
            cursor = conn.cursor()

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

            if not is_transient_error(e):
                log.error(
                    f"[{db_key}] non-transient error (permission/syntax/object-not-found) "
                    f"-- not retrying. Full detail logged to {LOG_FILE}. "
                    f"Check that the login has SELECT on files/documents/extractionDetails "
                    f"and CREATE TABLE permission in tempdb for this database."
                )
                break

            if attempt < MAX_RETRIES:
                sleep_for = RETRY_BACKOFF_SECONDS * attempt
                log.info(f"[{db_key}] retrying in {sleep_for}s "
                         f"(transient: blocking lock or timeout, not a script bug)")
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
    print(f"\n{C.CYAN}{C.BOLD}Available databases:{C.RESET}")
    keys = list(DATABASES.keys())
    for i, k in enumerate(keys, start=1):
        print(f"  {C.GREEN}{i}.{C.RESET} {DATABASES[k]['label']}")
    print(f"  {C.GREEN}{len(keys)+1}.{C.RESET} ALL")

    raw = input(f"\n{C.CYAN}Select database(s) by number (comma-separated, or 'all'): {C.RESET}").strip().lower()
    if raw in ("all", str(len(keys) + 1)):
        return keys

    selected = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        if not part.isdigit() or not (1 <= int(part) <= len(keys)):
            print(f"{C.YELLOW}Ignoring invalid selection: '{part}'{C.RESET}")
            continue
        selected.append(keys[int(part) - 1])

    if not selected:
        print(f"{C.RED}No valid database selected. Exiting.{C.RESET}")
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


def compute_period_label(start_date_str: str, end_date_str: str) -> str:
    """
    start_date is inclusive, end_date is exclusive (matches the query semantics).
    Examples:
      2026-05-01 -> 2026-06-01            => "May 2026"
      2026-05-01 -> 2026-07-01            => "May - June 2026"
      2026-11-01 -> 2027-02-01            => "November 2026 - January 2027"
    """
    from datetime import timedelta

    start = datetime.strptime(start_date_str, "%Y-%m-%d")
    end = datetime.strptime(end_date_str, "%Y-%m-%d")

    last_included = end - timedelta(days=1)
    start_month, start_year = start.strftime("%B"), start.year
    end_month, end_year = last_included.strftime("%B"), last_included.year

    if (start.year, start.month) == (last_included.year, last_included.month):
        return f"{start_month} {start_year}"
    if start_year == end_year:
        return f"{start_month} - {end_month} {start_year}"
    return f"{start_month} {start_year} - {end_month} {end_year}"


def prompt_date_range_for(db_key: str) -> tuple:
    print(f"\nUpload date range for {DATABASES[db_key]['label']}:")
    start_date = prompt_date("Start date (inclusive)")
    end_date = prompt_date("End date (exclusive)")
    return start_date, end_date


def print_grid_table(headers: list, rows: list):
    """Minimal dependency-free ASCII grid table (no tabulate needed), with
    ANSI coloring on the console (auto-disabled when not a real terminal)."""
    all_rows = [headers] + rows
    # Widths must be computed from PLAIN text (no color codes), otherwise
    # the invisible escape sequences would throw off column alignment.
    col_widths = [
        max(len(str(row[i])) for row in all_rows)
        for i in range(len(headers))
    ]

    def sep_line():
        line = "+" + "+".join("-" * (w + 2) for w in col_widths) + "+"
        return f"{C.GRAY}{line}{C.RESET}"

    def fmt_header():
        cells = [str(h).ljust(col_widths[i]) for i, h in enumerate(headers)]
        colored = [f"{C.CYAN}{C.BOLD}{c}{C.RESET}" for c in cells]
        return f"{C.GRAY}|{C.RESET} " + f" {C.GRAY}|{C.RESET} ".join(colored) + f" {C.GRAY}|{C.RESET}"

    def fmt_data_row(row):
        cells = []
        for i, cell in enumerate(row):
            padded = str(cell).ljust(col_widths[i])
            if i == 0:
                colored = f"{C.MAGENTA}{C.BOLD}{padded}{C.RESET}"
            elif str(cell).strip().upper() == "N/A":
                colored = f"{C.DIM}{padded}{C.RESET}"
            else:
                colored = f"{C.GREEN}{padded}{C.RESET}"
            cells.append(colored)
        return f"{C.GRAY}|{C.RESET} " + f" {C.GRAY}|{C.RESET} ".join(cells) + f" {C.GRAY}|{C.RESET}"

    print(sep_line())
    print(fmt_header())
    print(sep_line())
    for row in rows:
        print(fmt_data_row(row))
    print(sep_line())


def build_table_rows(selected_dbs: list, results: dict, date_ranges: dict) -> list:
    table_rows = []
    for stage_label, field in STAGE_ORDER:
        row = [stage_label]
        for k in selected_dbs:
            if field == "Month":
                start_date, end_date = date_ranges[k]
                val = compute_period_label(start_date, end_date)
            else:
                val = results.get(k, {}).get(field)
                val = val if val is not None else "N/A"
            row.append(val)
        table_rows.append(row)
    return table_rows


def print_report(selected_dbs: list, results: dict, date_ranges: dict, title: str):
    headers = ["Stage"] + [DATABASES[k]["label"] for k in selected_dbs]
    table_rows = build_table_rows(selected_dbs, results, date_ranges)

    print(f"\n{C.BLUE}{C.BOLD}{'=' * 70}{C.RESET}")
    print(f"{C.BLUE}{C.BOLD} {title}{C.RESET}")
    print(f"{C.BLUE}{C.BOLD}{'=' * 70}{C.RESET}")
    print_grid_table(headers, table_rows)


def build_email_html(selected_dbs: list, results: dict, date_ranges: dict) -> str:
    """Builds a clean, professional HTML table (email clients don't render
    ANSI colors, so this is a separate plain-HTML rendering of the same data
    used by print_grid_table)."""
    headers = ["Stage"] + [DATABASES[k]["label"] for k in selected_dbs]
    table_rows = build_table_rows(selected_dbs, results, date_ranges)

    th_cells = "".join(
        f'<th style="padding:8px 12px;border:1px solid #ccc;background:#2f5597;'
        f'color:#ffffff;text-align:left;font-family:Segoe UI,Arial,sans-serif;font-size:13px;">'
        f'{h}</th>'
        for h in headers
    )

    body_rows = ""
    for row in table_rows:
        tds = "".join(
            f'<td style="padding:8px 12px;border:1px solid #ccc;'
            f'font-family:Segoe UI,Arial,sans-serif;font-size:13px;'
            f'{"font-weight:600;background:#f2f2f2;" if i == 0 else ""}">'
            f'{cell}</td>'
            for i, cell in enumerate(row)
        )
        body_rows += f"<tr>{tds}</tr>"

    report_date = datetime.now().strftime("%d-%b-%Y")

    html = f"""\
<html>
<body style="font-family:Segoe UI,Arial,sans-serif;font-size:14px;color:#222;">
<p>Hi Team,</p>
<p>Please find below the Processing Status report generated on {report_date}.</p>
<table style="border-collapse:collapse;margin:12px 0;">
<thead><tr>{th_cells}</tr></thead>
<tbody>{body_rows}</tbody>
</table>
<p>Regards,<br>Automated Reporting</p>
</body>
</html>
"""
    return html


def send_report_email(html_body: str, subject: str, to_list: list, cc_list: list):
    msg = MIMEMultipart("alternative")
    msg["From"] = MAIL_FROM
    msg["To"] = "; ".join(to_list)
    if cc_list:
        msg["Cc"] = "; ".join(cc_list)
    msg["Subject"] = subject
    msg.attach(MIMEText(html_body, "html"))

    all_recipients = to_list + cc_list

    password = get_mail_password()
    context = ssl.create_default_context()
    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.ehlo()
        server.starttls(context=context)
        server.ehlo()
        server.login(MAIL_FROM, password)
        server.sendmail(MAIL_FROM, all_recipients, msg.as_string())


def prompt_send_email(selected_dbs: list, results: dict, date_ranges: dict):
    choice = input(
        f"\n{C.CYAN}Do you want to send this report via email? (y/n): {C.RESET}"
    ).strip().lower()
    if choice != "y":
        return

    default_to = parse_addr_list(DEFAULT_MAIL_TO)
    default_cc = parse_addr_list(DEFAULT_MAIL_CC)

    print(f"{C.GRAY}Default To: {'; '.join(default_to) or '(none)'}{C.RESET}")
    print(f"{C.GRAY}Default Cc: {'; '.join(default_cc) or '(none)'}{C.RESET}")

    extra_to_raw = input(
        f"{C.CYAN}Additional To recipients, semicolon-separated (Enter to skip): {C.RESET}"
    ).strip()
    extra_cc_raw = input(
        f"{C.CYAN}Additional Cc recipients, semicolon-separated (Enter to skip): {C.RESET}"
    ).strip()

    to_list = default_to + parse_addr_list(extra_to_raw)
    cc_list = default_cc + parse_addr_list(extra_cc_raw)

    # de-dupe while preserving order
    to_list = list(dict.fromkeys(to_list))
    cc_list = [addr for addr in dict.fromkeys(cc_list) if addr not in to_list]

    if not to_list:
        print(f"{C.RED}No To recipients configured or entered -- not sending.{C.RESET}")
        return

    subject = f"Processing Status Report - {datetime.now().strftime('%d-%b-%Y')}"
    html_body = build_email_html(selected_dbs, results, date_ranges)

    print(f"{C.YELLOW}Sending email to: {', '.join(to_list)}"
          f"{' | Cc: ' + ', '.join(cc_list) if cc_list else ''} ...{C.RESET}")
    try:
        send_report_email(html_body, subject, to_list, cc_list)
        print(f"{C.GREEN}Email sent successfully.{C.RESET}")
    except ValueError as e:
        print(f"{C.RED}Mail config error: {e}{C.RESET}")
    except smtplib.SMTPAuthenticationError as e:
        print(f"{C.RED}Mail auth failed: {e}{C.RESET}")
        print(f"{C.GRAY}-> Check MAIL_FROM/MAIL_PASSWORD_B64. If MFA is enabled, use an App Password.{C.RESET}")
    except smtplib.SMTPException as e:
        print(f"{C.RED}SMTP error: {e}{C.RESET}")
    except Exception as e:
        print(f"{C.RED}Unexpected error sending mail: {e}{C.RESET}")


def main():
    print(f"{C.BLUE}{C.BOLD}{'=' * 70}{C.RESET}")
    print(f"{C.BLUE}{C.BOLD} Processing Status Report{C.RESET}")
    print(f"{C.BLUE}{C.BOLD}{'=' * 70}{C.RESET}")

    selected_dbs = prompt_database_selection()

    same_range = "y"
    if len(selected_dbs) > 1:
        same_range = input(
            f"\n{C.CYAN}Use the SAME upload-date range for all selected databases? (y/n): {C.RESET}"
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
        print(f"\n{C.YELLOW}Running query on {DATABASES[k]['label']} "
              f"[{start_date} -> {end_date}] ...{C.RESET}")
        results[k] = run_status_query(k, start_date, end_date)

    # A DB counts as "failed" if run_status_query exhausted its internal
    # retries and returned an empty dict (see run_status_query / log.error
    # "gave up after N attempts").
    failed_dbs = [k for k in selected_dbs if not results.get(k)]

    title = "Processing Status" if not failed_dbs else "Processing Status (partial -- some DBs failed)"
    print_report(selected_dbs, results, date_ranges, f"{title} ({datetime.now().strftime('%d-%b-%Y')})")

    if failed_dbs:
        failed_labels = ", ".join(DATABASES[k]["label"] for k in failed_dbs)
        print(f"\n{C.RED}{C.BOLD}Failed to fetch results for: {failed_labels}{C.RESET}")
        print(f"{C.GRAY}(see {LOG_FILE} for the full error on each){C.RESET}")

        retry_choice = input(
            f"\n{C.CYAN}Retry just the failed database(s) once? (y/n): {C.RESET}"
        ).strip().lower()

        if retry_choice == "y":
            still_failed = []
            for k in failed_dbs:
                start_date, end_date = date_ranges[k]
                print(f"\n{C.YELLOW}Retrying {DATABASES[k]['label']} "
                      f"[{start_date} -> {end_date}] ...{C.RESET}")
                new_result = run_status_query(k, start_date, end_date)
                if new_result:
                    results[k] = new_result
                    print(f"{C.GREEN}[{k}] retry succeeded.{C.RESET}")
                else:
                    still_failed.append(k)
                    print(f"{C.RED}[{k}] retry failed again -- leaving as N/A.{C.RESET}")

            final_title = "Processing Status (final, after retry)"
            if still_failed:
                still_failed_labels = ", ".join(DATABASES[k]["label"] for k in still_failed)
                final_title += f" -- still failed: {still_failed_labels}"
            print_report(selected_dbs, results, date_ranges, f"{final_title} ({datetime.now().strftime('%d-%b-%Y')})")
        else:
            print(f"{C.GRAY}Skipping retry -- final result above includes N/A for failed DB(s).{C.RESET}")

    # Reached regardless of whether there were failures/retries -- always
    # offer to email whatever the final results ended up being.
    prompt_send_email(selected_dbs, results, date_ranges)


if __name__ == "__main__":
    main()
