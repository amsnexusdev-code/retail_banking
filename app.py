"""
Retail Banking demo app - single file.

Contains:
  1. DB table creation + seed script   (runs on startup, or: python app.py --init-db)
  2. Main server (FastAPI + uvicorn)
  3. Two business APIs
       GET  /accounts/{account_number}   -> balance enquiry
       POST /transactions                -> deposit / withdraw (updates balance)
  4. A "flaky" API: after FAIL_AFTER_SECONDS the chosen API is shut down and
     returns 503 until restarted via  POST /admin/restart/{api_name}
     (API restart is completely independent of logs / disk space)
  5. Periodic client: calls the GET and POST APIs every CLIENT_INTERVAL_SECONDS
     so logs are generated continuously (successes AND failures).
  6. Disk monitoring. Disk usage is driven ONLY by redundant log files in LOG_DIR:
        usage = BASELINE + (bytes of redundant files / DISK_CAPACITY_BYTES) * 100
     BASELINE starts at DISK_BASELINE_PERCENT (55) and, after every real cleanup that deletes
     files, becomes a new random value between DISK_AFTER_CLEANUP_MIN_PERCENT and
     DISK_AFTER_CLEANUP_MAX_PERCENT (default 50-55). It is saved to DISK_STATE_FILE so it
     survives restarts.
     Redundant = rotated retail_bank.log.N with N > keep, or demo_<epoch>_<i>.log older than
     DISK_CLEAN_AGE_DAYS (and other .tmp/.cache/.bak/.gz/.out names matching the same rule).
     No redundant files -> baseline. New redundant files -> usage jumps immediately.
       GET  /admin/disk-status      -> disk usage + redundant files
       POST /admin/cleanup-logs     -> delete redundant files (dry-run by default)
       POST /admin/create-demo-logs -> create back-dated dummy logs (demo/testing)
  7. Scheduled batch processing (accounts table -> Notepad-compatible TXT files).
     A scheduler thread (every BATCH_INTERVAL_SECONDS) fetches the accounts one record at a time
     (ordered by id) and renders each as a pipe-delimited ASCII/CRLF line. The rendered line is
     validated column by column. On the first formatting problem the job is PAUSED, the exact
     record / column / value / issue is logged ("BATCH PROCESSING FAILURE ...") and published in
     the batch block of GET /health; nothing already processed is touched.
     TXT-stage defect: the database values are clean. While a record is being written to the TXT
     line, BATCH_TXT_ERROR_ROWS_PER_JOB random record(s) per job get a stray '@' appended into the holder
     name (a formatting defect of the TXT stage). The check catches it, the job pauses and the failure
     says the database value itself is fine. The database is only READ; set the variable to 0 to turn it off.
     After a person approves "Skip & Resume" (POST /admin/restart/batch_processing, the existing
     restart hook) the failing record is skipped and processing continues from the next record in
     a NEW file (batch_001.txt -> batch_002.txt -> ...). Files are never appended to after a failure.
     Progress (checkpoint, lines, skipped records) lives in the Neon database only:
       batch_jobs / batch_lines / batch_skipped. No generated file is ever written to the server
     filesystem. Overlapping runs are prevented by an in-process lock, a DB lease and a unique
     "one active job" index. When a job completes, the final Excel workbook (processed + skipped
     worksheets) is built in memory (build_batch_workbook) - see the note on downloads there.

Run:  uvicorn app:app --reload   (or: python app.py)
"""
import base64
import io
import json
import logging
import os
import random
import re
import shutil
import sys
import threading
import time
import unicodedata
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from logging.handlers import RotatingFileHandler
from urllib.parse import urlparse

import requests
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field

load_dotenv()

# --------------------------------------------------------------------------
# Config (.env)
# --------------------------------------------------------------------------
DATABASE_URL = os.environ["DATABASE_URL"]
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8000"))
LOG_DIR = os.getenv("LOG_DIR", "logs")
FLAKY_API = os.getenv("FLAKY_API", "post_transactions")  # or: get_account
FAIL_AFTER_SECONDS = int(os.getenv("FAIL_AFTER_SECONDS", "180"))
CLIENT_ENABLED = os.getenv("CLIENT_ENABLED", "true").lower() == "true"
CLIENT_INTERVAL_SECONDS = int(os.getenv("CLIENT_INTERVAL_SECONDS", "15"))
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")  # if set, /admin/* endpoints need header X-Admin-Token
LOG_MAX_BYTES = int(os.getenv("LOG_MAX_BYTES", "100000"))   # rotate every ~100 KB
LOG_BACKUP_COUNT = int(os.getenv("LOG_BACKUP_COUNT", "4"))  # keep 4 old versions, delete older
CLIENT_BASE_URL = os.getenv("CLIENT_BASE_URL", f"http://127.0.0.1:{PORT}")
DISK_LOG_INTERVAL_SECONDS = int(os.getenv("DISK_LOG_INTERVAL_SECONDS", "60"))
DISK_ALERT_PERCENT = float(os.getenv("DISK_ALERT_PERCENT", "80"))
DISK_SIMULATE_PERCENT = float(os.getenv("DISK_SIMULATE_PERCENT", "0"))  # 0 = real model value; TEST ONLY
# Disk model - keep these identical to the alert backend's settings
DISK_BASELINE_PERCENT = float(os.getenv("DISK_BASELINE_PERCENT", "55"))
DISK_CAPACITY_BYTES = int(os.getenv("DISK_CAPACITY_BYTES", "200000"))
DISK_CLEAN_AGE_DAYS = int(os.getenv("DISK_CLEAN_AGE_DAYS", "5"))
# After a real cleanup the baseline usage becomes a random value in this range (new one every cleanup)
DISK_AFTER_CLEANUP_MIN_PERCENT = float(os.getenv("DISK_AFTER_CLEANUP_MIN_PERCENT", "50"))
DISK_AFTER_CLEANUP_MAX_PERCENT = float(os.getenv("DISK_AFTER_CLEANUP_MAX_PERCENT", "55"))
DISK_STATE_FILE = os.getenv("DISK_STATE_FILE", ".disk_baseline")   # keeps the baseline across restarts
# Scheduled batch processing (accounts -> TXT files)
BATCH_ENABLED = os.getenv("BATCH_ENABLED", "true").lower() == "true"
BATCH_INTERVAL_SECONDS = int(os.getenv("BATCH_INTERVAL_SECONDS", "300"))      # scheduler period
BATCH_START_DELAY_SECONDS = int(os.getenv("BATCH_START_DELAY_SECONDS", "20"))  # first run after startup
BATCH_LEASE_SECONDS = int(os.getenv("BATCH_LEASE_SECONDS", "120"))            # job lease (renewed per record)
BATCH_RECORD_DELAY_SECONDS = float(os.getenv("BATCH_RECORD_DELAY_SECONDS", "0"))  # optional slow-down for demos
BATCH_DOWNLOAD_MAX_BYTES = int(os.getenv("BATCH_DOWNLOAD_MAX_BYTES", "3000000"))  # raw bytes of files published for auto-download
BATCH_TXT_ERROR_ROWS_PER_JOB = int(os.getenv("BATCH_TXT_ERROR_ROWS_PER_JOB", "1"))  # TXT-stage formatting defects per job; 0 = off
BATCH_TXT_BAD_CHAR = "@"
BATCH_COMPONENT = "batch_processing"   # name accepted by POST /admin/restart/{api_name} (= Skip & Resume)

# --------------------------------------------------------------------------
# Logging (console + rotating file)
# --------------------------------------------------------------------------
os.makedirs(LOG_DIR, exist_ok=True)
_fmt = logging.Formatter("%(asctime)s | %(levelname)-7s | %(name)s | %(message)s")
log = logging.getLogger("retail_bank")
log.setLevel(logging.INFO)
if not log.handlers:
    _fh = RotatingFileHandler(
        os.path.join(LOG_DIR, "retail_bank.log"), maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUP_COUNT
    )
    _fh.setFormatter(_fmt)
    _sh = logging.StreamHandler(sys.stdout)
    _sh.setFormatter(_fmt)
    log.addHandler(_fh)
    log.addHandler(_sh)

# --------------------------------------------------------------------------
# Database
# --------------------------------------------------------------------------
# Neon SQL-over-HTTP: all queries go out as HTTPS (port 443), no Postgres port needed.
NEON_SQL_URL = f"https://{urlparse(DATABASE_URL).hostname}/sql"
_session = requests.Session()


def sql(query, params=None):
    """Run one SQL statement over HTTPS. Returns the list of row dicts."""
    r = _session.post(
        NEON_SQL_URL,
        headers={
            "Neon-Connection-String": DATABASE_URL,
            "Content-Type": "application/json",
        },
        json={"query": query, "params": params or []},
        timeout=30,
    )
    if not r.ok:
        raise RuntimeError(f"Neon HTTP {r.status_code}: {r.text[:300]}")
    return r.json().get("rows", [])


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS accounts (
    id             SERIAL PRIMARY KEY,
    account_number VARCHAR(20) UNIQUE NOT NULL,
    holder_name    VARCHAR(100) NOT NULL,
    balance        NUMERIC(14,2) NOT NULL DEFAULT 0 CHECK (balance >= 0),
    status         VARCHAR(10) NOT NULL DEFAULT 'ACTIVE',
    last_txn_type  VARCHAR(10),
    last_txn_amount NUMERIC(14,2),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""

# Batch-processing tables (one statement per entry - Neon HTTP runs one statement per call)
BATCH_DDL = [
    """
    CREATE TABLE IF NOT EXISTS batch_jobs (
        job_id           SERIAL PRIMARY KEY,
        status           VARCHAR(12) NOT NULL,          -- RUNNING | PAUSED | COMPLETED
        start_account_id INT NOT NULL DEFAULT 0,
        last_account_id  INT NOT NULL DEFAULT 0,        -- checkpoint: last accounts.id fully handled
        last_record_no   INT NOT NULL DEFAULT 0,        -- sequence number of the last handled record
        processed_count  INT NOT NULL DEFAULT 0,
        skipped_count    INT NOT NULL DEFAULT 0,
        next_file_no     INT NOT NULL DEFAULT 1,        -- file the next processed record goes into
        file_line_count  INT NOT NULL DEFAULT 0,        -- records already in that file
        failure          JSONB,                         -- details of the record the job is paused on
        owner            VARCHAR(40),                   -- lease owner (fencing token)
        lease_until      TIMESTAMPTZ,
        started_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
        finished_at      TIMESTAMPTZ
    )
    """,
    # at most ONE active (RUNNING or PAUSED) job at any time -> overlapping jobs are impossible
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_batch_one_active ON batch_jobs ((1)) WHERE status IN ('RUNNING','PAUSED')",
    """
    CREATE TABLE IF NOT EXISTS batch_lines (
        job_id     INT NOT NULL,
        record_no  INT NOT NULL,
        file_no    INT NOT NULL,
        account_id INT NOT NULL,
        line       TEXT NOT NULL,
        PRIMARY KEY (job_id, record_no)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS batch_skipped (
        job_id         INT NOT NULL,
        record_no      INT NOT NULL,
        account_id     INT NOT NULL,
        account_number TEXT,
        column_name    TEXT,
        value_shown    TEXT,
        issue          TEXT,
        failed_at      TIMESTAMPTZ,
        skipped_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (job_id, record_no)
    )
    """,
]

SEED_ACCOUNTS = [
    ("ACC1001", "Aarav Sharma", 25000),
    ("ACC1002", "Priya Das", 48000),
    ("ACC1003", "Rohan Mehta", 12500),
    ("ACC1004", "Ananya Roy", 90000),
    ("ACC1005", "Karan Singh", 5000),
]


def init_db():
    """Table creation script + seed data (idempotent)."""
    sql(CREATE_TABLE_SQL)
    params, values = [], []
    for i, (acc, name, bal) in enumerate(SEED_ACCOUNTS):
        values.append(f"(${i*3+1}, ${i*3+2}, ${i*3+3})")
        params += [acc, name, str(bal)]
    sql(
        "INSERT INTO accounts (account_number, holder_name, balance) VALUES "
        + ", ".join(values)
        + " ON CONFLICT (account_number) DO NOTHING",
        params,
    )
    for stmt in BATCH_DDL:
        sql(stmt)
    log.info("DB ready (via Neon HTTP): table 'accounts' exists and seed data present; batch tables ready")


# --------------------------------------------------------------------------
# Flaky API controller
# --------------------------------------------------------------------------
class FlakyController:
    """Shuts the chosen API down FAIL_AFTER_SECONDS after (re)start."""

    def __init__(self, api_name, fail_after):
        self.api_name = api_name
        self.fail_after = fail_after
        self.up = True
        self._timer = None
        self._lock = threading.Lock()

    def start(self):
        with self._lock:
            if self._timer:
                self._timer.cancel()
            self.up = True
            self._timer = threading.Timer(self.fail_after, self._shutdown)
            self._timer.daemon = True
            self._timer.start()
        log.info("API '%s' STARTED - will auto-stop in %ss", self.api_name, self.fail_after)

    def _shutdown(self):
        with self._lock:
            self.up = False
        log.error("API '%s' AUTO-STOPPED (simulated failure) - now returning 503", self.api_name)

    def is_up(self, name):
        return True if name != self.api_name else self.up


flaky = FlakyController(FLAKY_API, FAIL_AFTER_SECONDS)


def ensure_up(name):
    if not flaky.is_up(name):
        log.error("Request to '%s' rejected: API is stopped", name)
        raise HTTPException(status_code=503, detail=f"API '{name}' is stopped")


# --------------------------------------------------------------------------
# Periodic client (generates GET + POST/update traffic and logs)
# --------------------------------------------------------------------------
def client_loop():
    time.sleep(5)  # let the server come up
    accounts = [a[0] for a in SEED_ACCOUNTS]
    log.info("Periodic client started (every %ss)", CLIENT_INTERVAL_SECONDS)
    while True:
        acc = random.choice(accounts)
        try:
            r = requests.get(f"{CLIENT_BASE_URL}/accounts/{acc}", timeout=10)
            lvl = logging.INFO if r.ok else logging.ERROR
            log.log(lvl, "CLIENT GET /accounts/%s -> %s %s", acc, r.status_code, r.text[:150])
        except Exception as e:
            log.error("CLIENT GET /accounts/%s failed: %s", acc, e)

        payload = {
            "account_number": random.choice(accounts),
            "type": random.choice(["deposit", "deposit", "withdraw"]),
            "amount": round(random.uniform(10, 500), 2),
        }
        try:
            r = requests.post(f"{CLIENT_BASE_URL}/transactions", json=payload, timeout=10)
            lvl = logging.INFO if r.ok else logging.ERROR
            log.log(lvl, "CLIENT POST /transactions %s -> %s %s", payload, r.status_code, r.text[:150])
        except Exception as e:
            log.error("CLIENT POST /transactions failed: %s", e)

        time.sleep(CLIENT_INTERVAL_SECONDS)


# --------------------------------------------------------------------------
# Disk monitoring - usage is derived from redundant log files only
# --------------------------------------------------------------------------
_LOG_NAME_RE = re.compile(r"retail_bank\.log(?:\.(\d+))?$")
_DELETABLE_RE = re.compile(r"\.(log|tmp|cache|bak|gz|out)(\.\d+)?$", re.IGNORECASE)
_ROTATED_RE = re.compile(r"\.log\.(\d+)$", re.IGNORECASE)
_DEMO_RE = re.compile(r"^demo_(\d{9,11})_\d+\.log$")

# Baseline usage (the figure when no redundant files exist). Starts at DISK_BASELINE_PERCENT and is
# re-rolled to a random value in [DISK_AFTER_CLEANUP_MIN_PERCENT, DISK_AFTER_CLEANUP_MAX_PERCENT]
# after every real cleanup that deletes files. Saved to DISK_STATE_FILE so a restart keeps it.
_baseline_pct = DISK_BASELINE_PERCENT
_baseline_lock = threading.Lock()


def _load_baseline():
    global _baseline_pct
    try:
        with open(DISK_STATE_FILE, "r", encoding="utf-8") as fh:
            v = float(fh.read().strip())
        if 0 <= v <= 100:
            _baseline_pct = v
            log.info("DISK baseline restored from %s: %s%%", DISK_STATE_FILE, v)
    except (OSError, ValueError):
        pass   # no saved value yet -> keep DISK_BASELINE_PERCENT


def _roll_baseline():
    """New random baseline after a cleanup; persisted best-effort."""
    global _baseline_pct
    lo, hi = sorted((DISK_AFTER_CLEANUP_MIN_PERCENT, DISK_AFTER_CLEANUP_MAX_PERCENT))
    with _baseline_lock:
        _baseline_pct = round(random.uniform(lo, hi), 1)
        value = _baseline_pct
    try:
        with open(DISK_STATE_FILE, "w", encoding="utf-8") as fh:
            fh.write(str(value))
    except OSError as e:
        log.error("DISK baseline could not be saved to %s: %s", DISK_STATE_FILE, e)
    return value


_load_baseline()


def _log_versions():
    """retail_bank.log (version 0) and rotated retail_bank.log.N files with their sizes."""
    out = []
    for fn in os.listdir(LOG_DIR):
        m = _LOG_NAME_RE.fullmatch(fn)
        if m:
            p = os.path.join(LOG_DIR, fn)
            out.append({"name": fn, "path": p, "size": os.path.getsize(p), "version": int(m.group(1) or 0)})
    return out


def _redundant_files(keep=LOG_BACKUP_COUNT):
    """Files in LOG_DIR that are safe to delete: rotated logs beyond the newest `keep`,
    and demo_<epoch>_<i>.log files older than DISK_CLEAN_AGE_DAYS. Never the live log."""
    cutoff = time.time() - DISK_CLEAN_AGE_DAYS * 86400
    out = []
    for fn in os.listdir(LOG_DIR):
        p = os.path.join(LOG_DIR, fn)
        if not os.path.isfile(p) or os.path.islink(p) or not _DELETABLE_RE.search(fn):
            continue
        rot = _ROTATED_RE.search(fn)
        demo = _DEMO_RE.match(fn)
        if rot:
            eligible = int(rot.group(1)) > keep
        else:
            eligible = bool(demo) and int(demo.group(1)) <= cutoff
        if eligible:
            out.append({"name": fn, "path": p, "size": os.path.getsize(p)})
    return out


def _disk_state(keep=LOG_BACKUP_COUNT):
    extra = _redundant_files(keep)
    extra_bytes = sum(f["size"] for f in extra)
    pct = round(min(100.0, _baseline_pct + extra_bytes / max(1, DISK_CAPACITY_BYTES) * 100), 1)
    simulated = DISK_SIMULATE_PERCENT > 0
    if simulated:
        pct = DISK_SIMULATE_PERCENT
    size = sum(os.path.getsize(os.path.join(LOG_DIR, fn)) for fn in os.listdir(LOG_DIR)
               if os.path.isfile(os.path.join(LOG_DIR, fn)))
    return {"usage_percent": pct, "extra": extra, "extra_bytes": extra_bytes,
            "log_dir_size_bytes": size, "simulated": simulated}


def disk_log_once(keep=LOG_BACKUP_COUNT, note=""):
    st = _disk_state(keep)
    pct = st["usage_percent"]
    lvl = logging.WARNING if pct >= DISK_ALERT_PERCENT else logging.INFO
    log.log(lvl, "DISK usage=%s%% extra_log_files=%s extra_bytes=%s log_dir_size=%s%s%s",
            pct, len(st["extra"]), st["extra_bytes"], st["log_dir_size_bytes"],
            " (SIMULATED)" if st["simulated"] else "", f" ({note})" if note else "")
    return st


def disk_log_loop():
    time.sleep(10)  # let the server come up
    while True:
        try:
            disk_log_once()
        except Exception as e:
            log.error("DISK monitor failed: %s", e)
        time.sleep(DISK_LOG_INTERVAL_SECONDS)


def _check_admin(token: str):
    if ADMIN_TOKEN and token != ADMIN_TOKEN:
        raise HTTPException(403, "Invalid admin token")


# --------------------------------------------------------------------------
# Scheduled batch processing: accounts -> Notepad-compatible TXT files
# --------------------------------------------------------------------------
# TXT layout (predefined): ASCII, CRLF line endings, "|" delimiter, one header line per file, one
# line per account. Every column has a fixed specification; the rendered text of each column is
# checked against it BEFORE the line is accepted.
BATCH_DELIM = "|"
# (db column, header, nullable, max length, allowed characters, full pattern, human description)
BATCH_COLUMNS = [
    ("account_number", "ACCOUNT_NUMBER", False, 20, r"[A-Z0-9]", r"[A-Z0-9]{1,20}",
     "1-20 uppercase letters/digits"),
    ("holder_name", "HOLDER_NAME", False, 100, r"[A-Za-z .'\-]", r"[A-Za-z][A-Za-z .'\-]*",
     "letters, space, '.', apostrophe, '-' (must start with a letter)"),
    ("balance", "BALANCE", False, 15, r"[0-9.]", r"\d{1,12}\.\d{2}",
     "amount with exactly 2 decimals, e.g. 25000.00"),
    ("status", "STATUS", False, 10, r"[A-Z]", r"[A-Z]{1,10}", "1-10 uppercase letters"),
    ("last_txn_type", "LAST_TXN_TYPE", True, 10, r"[a-z]", r"deposit|withdraw",
     "'deposit' or 'withdraw' (or empty)"),
    ("last_txn_amount", "LAST_TXN_AMOUNT", True, 15, r"[0-9.]", r"\d{1,12}\.\d{2}",
     "amount with exactly 2 decimals (or empty)"),
    ("created_at", "CREATED_AT", False, 20, r"[0-9T:\-Z]", r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z",
     "UTC timestamp YYYY-MM-DDTHH:MM:SSZ"),
    ("updated_at", "UPDATED_AT", False, 20, r"[0-9T:\-Z]", r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z",
     "UTC timestamp YYYY-MM-DDTHH:MM:SSZ"),
]
BATCH_HEADER_LINE = BATCH_DELIM.join(c[1] for c in BATCH_COLUMNS)

# Records are fetched ONE at a time, in id order. Numbers / timestamps are rendered by the database
# so their text form is fixed; every other value is passed through untouched.
BATCH_FETCH_SQL = """
SELECT id, account_number, holder_name, balance::text AS balance, status, last_txn_type,
       last_txn_amount::text AS last_txn_amount,
       to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
       to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
FROM accounts WHERE id > $1::int ORDER BY id LIMIT 1
"""

# new job: starts after the last account of the newest COMPLETED job, only if there is something new.
# The unique "one active job" index turns a concurrent insert into a no-op.
BATCH_NEW_JOB_SQL = """
INSERT INTO batch_jobs (status, start_account_id, last_account_id, owner, lease_until)
SELECT 'RUNNING', s.m, s.m, $1::text, now() + make_interval(secs => $2::int)
FROM (SELECT COALESCE(MAX(last_account_id), 0) AS m FROM batch_jobs WHERE status = 'COMPLETED') s
WHERE EXISTS (SELECT 1 FROM accounts WHERE id > s.m)
ON CONFLICT DO NOTHING
RETURNING job_id
"""

# take over a RUNNING job whose lease expired (crashed / restarted process) -> continues at checkpoint
BATCH_CLAIM_SQL = """
UPDATE batch_jobs SET owner = $2::text, lease_until = now() + make_interval(secs => $3::int), updated_at = now()
WHERE job_id = $1::int AND status = 'RUNNING' AND (lease_until IS NULL OR lease_until < now())
RETURNING job_id
"""

# store one processed line + advance the checkpoint in ONE atomic statement (fenced by the lease owner)
BATCH_RECORD_SQL = """
WITH ins AS (
    INSERT INTO batch_lines (job_id, record_no, file_no, account_id, line)
    SELECT job_id, $2::int, next_file_no, $3::int, $4::text
    FROM batch_jobs WHERE job_id = $1::int AND status = 'RUNNING' AND owner = $5::text
)
UPDATE batch_jobs
SET last_account_id = $3::int, last_record_no = $2::int, processed_count = processed_count + 1,
    file_line_count = file_line_count + 1,
    lease_until = now() + make_interval(secs => $6::int), updated_at = now()
WHERE job_id = $1::int AND status = 'RUNNING' AND owner = $5::text
RETURNING job_id
"""

# formatting failure: pause. The file that was being written is closed (next record -> next file).
# The checkpoint is NOT advanced, so every already processed record stays exactly as it is.
BATCH_PAUSE_SQL = """
UPDATE batch_jobs
SET status = 'PAUSED', failure = $2::jsonb,
    next_file_no = next_file_no + CASE WHEN file_line_count > 0 THEN 1 ELSE 0 END,
    file_line_count = 0, owner = NULL, lease_until = NULL, updated_at = now()
WHERE job_id = $1::int AND status = 'RUNNING' AND owner = $3::text
RETURNING job_id
"""

# Skip & Resume: copy the failure into batch_skipped, move the checkpoint PAST the failing record,
# and take the job back to RUNNING - all in one statement.
BATCH_SKIP_SQL = """
WITH j AS (
    SELECT job_id, failure FROM batch_jobs
    WHERE job_id = $1::int AND status = 'PAUSED' AND (failure->>'record_number')::int = $2::int
), ins AS (
    INSERT INTO batch_skipped (job_id, record_no, account_id, account_number, column_name, value_shown, issue, failed_at)
    SELECT job_id, (failure->>'record_number')::int, (failure->>'db_id')::int, failure->>'account_number',
           failure->>'column', failure->>'value', failure->>'issue', (failure->>'detected_at')::timestamptz
    FROM j
    ON CONFLICT DO NOTHING
)
UPDATE batch_jobs b
SET status = 'RUNNING', last_account_id = (j.failure->>'db_id')::int,
    last_record_no = (j.failure->>'record_number')::int, skipped_count = b.skipped_count + 1,
    failure = NULL, owner = $3::text, lease_until = now() + make_interval(secs => $4::int), updated_at = now()
FROM j WHERE b.job_id = j.job_id AND b.status = 'PAUSED'
RETURNING b.job_id, b.next_file_no, b.last_record_no
"""

BATCH_COMPLETE_SQL = """
UPDATE batch_jobs SET status = 'COMPLETED', owner = NULL, lease_until = NULL,
       finished_at = now(), updated_at = now()
WHERE job_id = $1::int AND status = 'RUNNING' AND owner = $2::text
RETURNING job_id
"""

_batch_lock = threading.Lock()          # one batch execution per process at any time
_batch_state_lock = threading.Lock()
_batch_workbook_job = {"id": None}      # job whose final workbook was built successfully
_batch_workbook_cache = {"id": None, "bytes": None}   # in memory only (never on disk)
_batch_snapshot = {"enabled": BATCH_ENABLED, "status": "IDLE", "job_id": None, "processed": 0,
                   "skipped": 0, "last_record": 0, "next_file": None, "files": [],
                   "failure": None, "workbook_ready": False, "downloads": []}


def _i(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _json(value):
    if isinstance(value, (dict, list)) or value is None:
        return value
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return None


def _batch_file_name(file_no):
    return f"batch_{int(file_no):03d}.txt"


def _show(value, limit=120):
    """Escaped, single-line, ASCII-only rendering of a value (so a bad character can be SEEN)."""
    if value is None:
        return "NULL"
    s = ascii(str(value))
    return s if len(s) <= limit else s[: limit - 3] + "..."


def _plain(value):
    s = _show(value)
    return s[1:-1] if len(s) >= 2 and s[0] == s[-1] and s[0] in "'\"" else s


def _char_label(ch):
    return f"U+{ord(ch):04X} {unicodedata.name(ch, 'unnamed character')}"


def _field_issue(spec, text):
    """Checks the rendered text of ONE column against its specification.
    Returns (issue, position) or None. Position is 1-based within the value (0 = whole value)."""
    column, _hdr, nullable, max_len, allowed, pattern, expected = spec
    if text == "":
        return None if nullable else ("empty value in a mandatory column", 0)
    for pos, ch in enumerate(text, start=1):
        o = ord(ch)
        cat = unicodedata.category(ch)
        if ch == "\r" or ch == "\n":
            return (f"line break ({'CR' if ch == chr(13) else 'LF'}) inside the value at position {pos}; "
                    f"it would split the record across TXT lines", pos)
        if ch == "\t":
            return (f"TAB character at position {pos}; it misaligns the TXT columns", pos)
        if ch == BATCH_DELIM:
            return (f"delimiter '{BATCH_DELIM}' inside the value at position {pos}; "
                    f"it would add an extra column to the TXT line", pos)
        if o == 0xFFFD:
            return (f"encoding error: replacement character U+FFFD at position {pos} "
                    f"(the source text was decoded incorrectly)", pos)
        if o < 32 or o == 127 or cat in ("Cc", "Cf", "Cs", "Co", "Cn", "Zl", "Zp"):
            return (f"invisible/control character {_char_label(ch)} at position {pos}", pos)
        if o > 127:
            return (f"non-ASCII character {_char_label(ch)} at position {pos}; it cannot be written to the "
                    f"ASCII TXT file (encoding issue / unexpected symbol)", pos)
    if text != text.strip(" "):
        return ("leading or trailing space in the value", 1 if text.startswith(" ") else len(text))
    if "  " in text:
        return (f"consecutive spaces at position {text.index('  ') + 1}", text.index("  ") + 1)
    if len(text) > max_len:
        return (f"value is {len(text)} characters long, maximum for this column is {max_len}", max_len + 1)
    for pos, ch in enumerate(text, start=1):
        if not re.fullmatch(allowed, ch):
            return (f"unexpected symbol {ch!r} at position {pos}; expected {expected}", pos)
    if not re.fullmatch(pattern, text):
        return (f"value does not match the required format; expected {expected}", 0)
    return None


def _validate_batch_record(row, job_id, record_no):
    """Renders one account row as a TXT line and validates the RENDERED text.
    Returns (line, None) when clean, or (None, failure_dict) for the first affected column."""
    fields, problems = [], []
    for spec in BATCH_COLUMNS:
        raw = row.get(spec[0])
        text = "" if raw is None else str(raw)
        fields.append(text)
        found = _field_issue(spec, text)
        if found:
            problems.append((spec, raw, found))

    def failure(column, shown, issue, position):
        return {
            "job_id": job_id, "record_number": record_no, "db_id": _i(row.get("id")),
            "account_number": _plain(row.get("account_number")), "column": column,
            "value": shown, "issue": issue, "position": position,
            "detected_at": datetime.now(timezone.utc).isoformat(),
        }

    if problems:
        spec, raw, (issue, position) = problems[0]
        if len(problems) > 1:
            issue += f" (+{len(problems) - 1} more invalid column(s): {', '.join(p[0][0] for p in problems[1:])})"
        return None, failure(spec[0], _show(raw), issue, position)

    # whole-line backstop: the line must split back into exactly the same columns and be pure ASCII
    line = BATCH_DELIM.join(fields)
    if "\r" in line or "\n" in line or line.split(BATCH_DELIM) != fields:
        return None, failure("(whole line)", _show(line), "rendered line does not split back into the expected "
                             f"{len(BATCH_COLUMNS)} columns", 0)
    try:
        line.encode("ascii")
    except UnicodeEncodeError as e:
        return None, failure("(whole line)", _show(line), f"line cannot be encoded as ASCII: {e.reason} "
                             f"at offset {e.start}", e.start + 1)
    return line, None


def _txt_defect_records(job):
    """Record numbers (1-based, in id order) that get the TXT-stage formatting defect in this job.
    Deterministic per job (seeded with the job id), so a restart continues with the same records."""
    k = max(0, BATCH_TXT_ERROR_ROWS_PER_JOB)
    if not k:
        return set()
    n = _i(sql("SELECT COUNT(*) AS n FROM accounts WHERE id > $1::int", [str(_i(job["start_account_id"]))])[0]["n"])
    return set(random.Random(_i(job["job_id"])).sample(range(1, n + 1), min(k, n))) if n else set()


def _apply_txt_defect(row):
    """Simulates the formatting defect of the TXT stage: a stray character lands inside the holder name
    of the line being written. Returns (row_as_rendered, original_name). The database row is untouched."""
    original = str(row.get("holder_name") or "Unknown")
    mid = max(1, len(original) // 2)
    return {**row, "holder_name": f"{original[:mid]}{BATCH_TXT_BAD_CHAR}{original[mid:]}"}, original


def _batch_txt_bytes(lines):
    """Notepad-compatible content: header + lines, CRLF endings, ASCII (valid UTF-8 as well)."""
    return ("\r\n".join([BATCH_HEADER_LINE, *lines]) + "\r\n").encode("ascii")


def get_batch_file_bytes(job_id, file_no):
    """In-memory content of one TXT file (assembled from batch_lines on demand, never saved to disk).
    Returns (file_name, bytes) or None. Ready to be served by a download endpoint - see NOTE below."""
    rows = sql("SELECT line FROM batch_lines WHERE job_id = $1::int AND file_no = $2::int ORDER BY record_no",
               [str(job_id), str(file_no)])
    if not rows:
        return None
    return _batch_file_name(file_no), _batch_txt_bytes([r["line"] for r in rows])


def build_batch_workbook(job_id):
    """Final Excel workbook for a job, built entirely in memory (bytes are returned, nothing is saved):
       sheet 'Processed Records' - every successfully processed record (with its TXT file name)
       sheet 'Skipped Records'   - every skipped record with the exact failure details.
    Downloads: no file is stored on disk. Closed TXT files and this workbook are published, base64
    encoded, in the "downloads" list of the batch block of GET /health (admin token only); the alert
    backend forwards it through GET /api/alerts/retail/status and the browser saves each item."""
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font
    except ImportError as e:                                   # pragma: no cover
        raise RuntimeError("openpyxl is not installed (add 'openpyxl' to requirements.txt)") from e
    processed = sql("SELECT record_no, file_no, account_id, line FROM batch_lines "
                    "WHERE job_id = $1::int ORDER BY record_no", [str(job_id)])
    skipped = sql("SELECT record_no, account_id, account_number, column_name, value_shown, issue, "
                  "to_char(failed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') AS failed_at, "
                  "to_char(skipped_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') AS skipped_at "
                  "FROM batch_skipped WHERE job_id = $1::int ORDER BY record_no", [str(job_id)])

    wb = Workbook()
    ws = wb.active
    ws.title = "Processed Records"
    ws.append(["Record No", "TXT File", "DB ID", *[c[1] for c in BATCH_COLUMNS]])
    for r in processed:
        parts = str(r["line"]).split(BATCH_DELIM)
        cells = []
        for spec, p in zip(BATCH_COLUMNS, parts):
            if spec[0] in ("balance", "last_txn_amount") and p:
                try:
                    p = float(Decimal(p))
                except InvalidOperation:
                    pass
            cells.append(p if p != "" else None)
        ws.append([_i(r["record_no"]), _batch_file_name(_i(r["file_no"])), _i(r["account_id"]), *cells])
    for col in ("G", "I"):                                     # BALANCE, LAST_TXN_AMOUNT
        for cell in ws[col][1:]:
            cell.number_format = "0.00"

    ws2 = wb.create_sheet("Skipped Records")
    ws2.append(["Record No", "DB ID", "Account Number", "Column", "Value", "Failure Details",
                "Failed At (UTC)", "Skipped At (UTC)"])
    for r in skipped:
        ws2.append([_i(r["record_no"]), _i(r["account_id"]), r["account_number"], r["column_name"],
                    r["value_shown"], r["issue"], r["failed_at"], r["skipped_at"]])

    for sheet in (ws, ws2):
        for cell in sheet[1]:
            cell.font = Font(bold=True)
        sheet.freeze_panes = "A2"
        for col in sheet.columns:
            width = max(len(str(c.value)) if c.value is not None else 0 for c in col[:200])
            sheet.column_dimensions[col[0].column_letter].width = min(max(12, width + 2), 80)
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def _download_item(job_id, name, mime, data, budget):
    """One browser-download item. Content is base64 (JSON-safe) and lives in memory only."""
    item = {"job_id": job_id, "name": name, "mime": mime, "size": len(data), "content_base64": None}
    if len(data) <= budget[0]:
        budget[0] -= len(data)
        item["content_base64"] = base64.b64encode(data).decode("ascii")
    else:
        item["omitted"] = f"larger than the remaining BATCH_DOWNLOAD_MAX_BYTES budget"
    return item


def _batch_downloads(job_id, status, next_file_no, files):
    """Files the browser should save automatically: every CLOSED TXT file (all of them once the job is
    COMPLETED) plus the final Excel workbook after completion. Built from the database on demand."""
    budget = [BATCH_DOWNLOAD_MAX_BYTES]
    out = []
    for f in files:
        n = _i(f["file_no"])
        if status != "COMPLETED" and n >= next_file_no:
            continue                      # the file still being written is not final yet
        got = get_batch_file_bytes(job_id, n)
        if got:
            out.append(_download_item(job_id, got[0], "text/plain", got[1], budget))
    if status == "COMPLETED":
        if _batch_workbook_cache["id"] != job_id:
            _batch_workbook_cache.update(id=job_id, bytes=build_batch_workbook(job_id))
        _batch_workbook_job["id"] = job_id
        out.append(_download_item(
            job_id, f"batch_final_job{job_id}.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", _batch_workbook_cache["bytes"], budget))
    return out


def _refresh_batch_snapshot():
    """Rebuilds the status block published in GET /health from the database. Never raises."""
    try:
        rows = sql("SELECT * FROM batch_jobs ORDER BY job_id DESC LIMIT 1")
        if not rows:
            snap = {"enabled": BATCH_ENABLED, "status": "IDLE", "job_id": None, "processed": 0, "skipped": 0,
                    "last_record": 0, "next_file": None, "files": [], "failure": None, "workbook_ready": False,
                    "downloads": []}
        else:
            job = rows[0]
            jid = _i(job["job_id"])
            files = sql("SELECT file_no, COUNT(*) AS records FROM batch_lines WHERE job_id = $1::int "
                        "GROUP BY file_no ORDER BY file_no", [str(jid)])
            status = job["status"]
            try:
                downloads = _batch_downloads(jid, status, _i(job["next_file_no"]), files)
            except Exception as e:
                downloads = []
                log.error("BATCH download files could not be prepared: %s", e)
            snap = {
                "enabled": BATCH_ENABLED, "status": status, "job_id": jid,
                "processed": _i(job["processed_count"]), "skipped": _i(job["skipped_count"]),
                "last_record": _i(job["last_record_no"]),
                "next_file": _batch_file_name(_i(job["next_file_no"])) if status != "COMPLETED" else None,
                "files": [{"name": _batch_file_name(_i(f["file_no"])), "records": _i(f["records"])} for f in files],
                "failure": _json(job.get("failure")) if status == "PAUSED" else None,
                "workbook_ready": status == "COMPLETED" and _batch_workbook_job["id"] == jid,
                "downloads": downloads,
            }
        with _batch_state_lock:
            _batch_snapshot.clear()
            _batch_snapshot.update(snap)
    except Exception as e:
        log.error("BATCH status refresh failed: %s", e)


def _batch_public_status(is_admin):
    with _batch_state_lock:
        snap = dict(_batch_snapshot)
    if not is_admin:
        snap["failure"] = None      # exact value of the bad record and the file contents are only for the admin/alert module
        snap["downloads"] = []
    return snap


def _process_batch_job(job_id, owner):
    """Sequentially handles the job's records. Returns when the job is paused, completed, or
    the lease was lost. The caller must hold _batch_lock."""
    rows = sql("SELECT * FROM batch_jobs WHERE job_id = $1::int", [str(job_id)])
    if not rows or rows[0]["status"] != "RUNNING" or rows[0]["owner"] != owner:
        return
    job = rows[0]
    last_id, rec_no = _i(job["last_account_id"]), _i(job["last_record_no"])
    file_no, file_lines = _i(job["next_file_no"]), _i(job["file_line_count"])
    try:
        defect_at = _txt_defect_records(job)
    except Exception as e:
        defect_at = set()
        log.error("BATCH job %s: TXT defect plan could not be read (%s); continuing without it", job_id, e)
    log.info("BATCH job %s processing from record %s into %s", job_id, rec_no + 1, _batch_file_name(file_no))
    _refresh_batch_snapshot()

    while True:
        fetched = sql(BATCH_FETCH_SQL, [str(last_id)])
        if not fetched:
            break
        row = fetched[0]
        rec_no += 1
        db_id = _i(row["id"])
        original_name = None
        if rec_no in defect_at:                       # TXT-stage defect: a stray character is added while writing
            row, original_name = _apply_txt_defect(row)
        line, failure = _validate_batch_record(row, job_id, rec_no)
        if failure and original_name is not None:
            failure["issue"] += (f" | database value {_show(original_name)} is correct; the character was added "
                                 f"while the record was being written to the TXT file")
        if failure:
            closed = _batch_file_name(file_no) if file_lines > 0 else None
            nxt = _batch_file_name(file_no + (1 if file_lines > 0 else 0))
            failure["closed_file"], failure["next_file"] = closed, nxt
            if not sql(BATCH_PAUSE_SQL, [str(job_id), json.dumps(failure), owner]):
                log.error("BATCH job %s lost its lease while pausing - stopping this execution", job_id)
                return
            log.error("BATCH PROCESSING FAILURE job=%s record=%s db_id=%s account=%s column=%s value=%s "
                      "issue=%s | job PAUSED, %s record(s) kept%s, waiting for Skip & Resume approval",
                      job_id, rec_no, db_id, failure["account_number"], failure["column"], failure["value"],
                      failure["issue"], file_lines, f" in {closed}" if closed else "")
            _refresh_batch_snapshot()
            return
        if not sql(BATCH_RECORD_SQL, [str(job_id), str(rec_no), str(db_id), line, owner, str(BATCH_LEASE_SECONDS)]):
            log.error("BATCH job %s lost its lease - stopping this execution", job_id)
            return
        last_id, file_lines = db_id, file_lines + 1
        if BATCH_RECORD_DELAY_SECONDS > 0:
            time.sleep(BATCH_RECORD_DELAY_SECONDS)

    if not sql(BATCH_COMPLETE_SQL, [str(job_id), owner]):
        log.error("BATCH job %s lost its lease before completion - stopping this execution", job_id)
        return
    try:
        _batch_workbook_cache.update(id=job_id, bytes=build_batch_workbook(job_id))
        _batch_workbook_job["id"] = job_id
        wb_note = f"final Excel workbook built in memory ({len(_batch_workbook_cache['bytes'])} bytes)"
    except Exception as e:
        wb_note = f"final Excel workbook could NOT be built: {e}"
        log.error("BATCH job %s %s", job_id, wb_note)
    log.info("BATCH job %s COMPLETED: %s record(s) handled, %s", job_id, rec_no, wb_note)
    _refresh_batch_snapshot()


def _run_batch_job(job_id, owner):
    """Thread entry used after Skip & Resume. Waits for the process-wide lock, then continues the job."""
    if not _batch_lock.acquire(timeout=30):
        log.warning("BATCH job %s resume is waiting for the running execution; the scheduler will pick it up", job_id)
        return
    try:
        _process_batch_job(job_id, owner)
    except Exception as e:
        log.error("BATCH job %s execution failed: %s", job_id, e)
    finally:
        _batch_lock.release()
        _refresh_batch_snapshot()


def _batch_tick():
    """One scheduled execution. Never overlaps with another one."""
    if not _batch_lock.acquire(blocking=False):
        log.warning("BATCH scheduled run skipped: the previous execution is still running")
        return
    try:
        owner = uuid.uuid4().hex
        active = sql("SELECT job_id, status FROM batch_jobs WHERE status IN ('RUNNING','PAUSED') "
                     "ORDER BY job_id DESC LIMIT 1")
        if active:
            jid = _i(active[0]["job_id"])
            if active[0]["status"] == "PAUSED":
                log.info("BATCH job %s is PAUSED - waiting for Skip & Resume approval", jid)
                _refresh_batch_snapshot()
                return
            if not sql(BATCH_CLAIM_SQL, [str(jid), owner, str(BATCH_LEASE_SECONDS)]):
                log.info("BATCH job %s is still held by a live execution - scheduled run skipped", jid)
                return
            log.warning("BATCH job %s recovered from its checkpoint after an interrupted execution", jid)
            _process_batch_job(jid, owner)
            return
        created = sql(BATCH_NEW_JOB_SQL, [owner, str(BATCH_LEASE_SECONDS)])
        if not created:
            log.info("BATCH nothing to process: no account records after the last completed job")
            return
        _process_batch_job(_i(created[0]["job_id"]), owner)
    except Exception as e:
        log.error("BATCH scheduled run failed: %s", e)
    finally:
        _batch_lock.release()
        _refresh_batch_snapshot()


def batch_scheduler_loop():
    time.sleep(BATCH_START_DELAY_SECONDS)   # let the server come up
    log.info("BATCH scheduler started (every %ss)", BATCH_INTERVAL_SECONDS)
    while True:
        _batch_tick()
        time.sleep(BATCH_INTERVAL_SECONDS)


def batch_skip_and_resume(job_id=0, record_no=0):
    """Skip & Resume (approved by a person). Skips the record the job is paused on and resumes from
    the next database record in a NEW sequentially numbered TXT file."""
    if not BATCH_ENABLED:
        raise HTTPException(404, "batch processing is disabled")
    paused = sql("SELECT job_id, failure FROM batch_jobs WHERE status = 'PAUSED' ORDER BY job_id DESC LIMIT 1")
    if not paused:
        raise HTTPException(409, "No paused batch job - nothing to skip")
    jid, failure = _i(paused[0]["job_id"]), _json(paused[0]["failure"]) or {}
    rec = _i(failure.get("record_number"))
    if job_id and job_id != jid:
        raise HTTPException(409, f"Paused job is {jid}, not {job_id}")
    if record_no and record_no != rec:
        raise HTTPException(409, f"Job {jid} is paused on record {rec}, not {record_no}")
    owner = uuid.uuid4().hex
    res = sql(BATCH_SKIP_SQL, [str(jid), str(rec), owner, str(BATCH_LEASE_SECONDS)])
    if not res:
        raise HTTPException(409, "The paused job changed or was already resumed")
    next_file = _batch_file_name(_i(res[0]["next_file_no"]))
    log.warning("BATCH Skip & Resume approved: job=%s skipped record=%s account=%s column=%s; resuming from "
                "record %s into %s", jid, rec, failure.get("account_number"), failure.get("column"),
                rec + 1, next_file)
    _refresh_batch_snapshot()
    threading.Thread(target=_run_batch_job, args=(jid, owner), daemon=True).start()
    return {"restarted": BATCH_COMPONENT, "action": "skip_and_resume", "status": "RUNNING", "job_id": jid,
            "skipped_record": rec, "skipped_account": failure.get("account_number"),
            "resumed_from_record": rec + 1, "next_file": next_file}


# --------------------------------------------------------------------------
# App + APIs
# --------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    flaky.start()
    threading.Thread(target=disk_log_loop, daemon=True).start()
    if CLIENT_ENABLED:
        threading.Thread(target=client_loop, daemon=True).start()
    if BATCH_ENABLED:
        _refresh_batch_snapshot()
        threading.Thread(target=batch_scheduler_loop, daemon=True).start()
    yield


app = FastAPI(title="Retail Banking", lifespan=lifespan)


class Transaction(BaseModel):
    account_number: str
    type: str = Field(pattern="^(deposit|withdraw)$")
    amount: float = Field(gt=0)


@app.get("/health")
def health(x_admin_token: str = Header(default="")):
    # "apis" is unchanged. "batch" is the batch-job status (served from memory, no DB call). The exact
    # failing value and "downloads" (closed TXT files + final Excel as base64, for the browser to save
    # automatically) are only included when the caller sends a valid X-Admin-Token (or none is configured).
    return {
        "status": "ok",
        "apis": {
            "get_account": flaky.is_up("get_account"),
            "post_transactions": flaky.is_up("post_transactions"),
        },
        "batch": _batch_public_status(not ADMIN_TOKEN or x_admin_token == ADMIN_TOKEN),
    }


@app.get("/accounts/{account_number}")
def get_account(account_number: str):
    ensure_up("get_account")
    rows = sql(
        """SELECT account_number, holder_name, balance, status, updated_at
           FROM accounts WHERE account_number = $1""",
        [account_number],
    )
    if not rows:
        raise HTTPException(404, "Account not found")
    row = rows[0]
    log.info("GET account %s balance=%s", account_number, row["balance"])
    return row


@app.post("/transactions")
def post_transaction(txn: Transaction):
    ensure_up("post_transactions")
    delta = txn.amount if txn.type == "deposit" else -txn.amount
    rows = sql(
        """UPDATE accounts
           SET balance = balance + $1, last_txn_type = $2, last_txn_amount = $3,
               updated_at = now()
           WHERE account_number = $4 AND status = 'ACTIVE' AND balance + $1 >= 0
           RETURNING account_number, balance, updated_at""",
        [f"{delta:.2f}", txn.type, f"{txn.amount:.2f}", txn.account_number],
    )
    if not rows:
        if not sql("SELECT 1 FROM accounts WHERE account_number = $1", [txn.account_number]):
            raise HTTPException(404, "Account not found")
        raise HTTPException(422, "Insufficient funds or account inactive")
    row = rows[0]
    log.info("POST txn %s %s %.2f -> new balance %s",
             txn.account_number, txn.type, txn.amount, row["balance"])
    return row


@app.post("/admin/restart/{api_name}")
def restart_api(api_name: str, job_id: int = 0, record_no: int = 0, x_admin_token: str = Header(default="")):
    """Restart hook used by the alert module. Protected by X-Admin-Token when ADMIN_TOKEN is set.
    For the flaky API it only touches the API state - never logs or disk.
    For 'batch_processing' it is the approved "Skip & Resume" of a paused batch job
    (optional job_id / record_no make sure the approval is for the record the person saw)."""
    _check_admin(x_admin_token)
    if api_name == BATCH_COMPONENT:
        return batch_skip_and_resume(job_id, record_no)
    if api_name != flaky.api_name:
        raise HTTPException(404, f"'{api_name}' is not the managed flaky API")
    flaky.start()
    log.warning("API '%s' RESTARTED via admin endpoint", api_name)
    return {"restarted": api_name, "auto_stop_in_seconds": FAIL_AFTER_SECONDS}


@app.get("/admin/disk-status")
def disk_status(keep: int = LOG_BACKUP_COUNT, x_admin_token: str = Header(default="")):
    """Disk usage (driven by redundant log files) plus the list of those files."""
    _check_admin(x_admin_token)
    st = disk_log_once(keep, "disk-status")
    return {"usage_percent": st["usage_percent"], "used_bytes": st["extra_bytes"],
            "total_bytes": DISK_CAPACITY_BYTES, "log_dir_size_bytes": st["log_dir_size_bytes"],
            "extra_versions": [f["name"] for f in st["extra"]], "extra_bytes": st["extra_bytes"],
            "simulated": st["simulated"]}


@app.post("/admin/cleanup-logs")
def cleanup_logs(dry_run: bool = True, keep: int = LOG_BACKUP_COUNT,
                 x_admin_token: str = Header(default="")):
    """Deletes only redundant files (see module docstring). Never the live log.
    Dry-run by default; pass dry_run=false to actually delete. A real run that deletes files
    sets a new random baseline usage (DISK_AFTER_CLEANUP_MIN/MAX_PERCENT, default 50-55%) and
    immediately logs the new, lower DISK usage line."""
    _check_admin(x_admin_token)
    victims = _redundant_files(keep)
    freed = sum(f["size"] for f in victims)
    deleted = 0
    if not dry_run:
        for f in victims:
            try:
                os.remove(f["path"])
                deleted += 1
            except OSError as e:
                log.error("DISK cleanup could not delete %s: %s", f["name"], e)
    log.warning("DISK log cleanup %s: %s file(s), %s bytes (keep newest %s versions)",
                "DRY-RUN" if dry_run else "EXECUTED", len(victims), freed, keep)
    if not dry_run and deleted:
        new_base = _roll_baseline()
        log.warning("DISK baseline after cleanup set to %s%%", new_base)
    after = disk_log_once(keep, "after cleanup") if not dry_run else _disk_state(keep)
    return {"dry_run": dry_run, "file_count": len(victims), "bytes": freed,
            "files": [f["name"] for f in victims], "usage_after_percent": after["usage_percent"]}


@app.post("/admin/create-demo-logs")
def create_demo_logs(count: int = 5, epoch: int = 0, x_admin_token: str = Header(default="")):
    """Demo helper: creates back-dated dummy logs demo_<epoch>_<i>.log (10 KB each) so they count
    as redundant. A fresh DISK usage line is logged immediately (usage jumps right away)."""
    _check_admin(x_admin_token)
    ts = epoch or int(time.time() - (DISK_CLEAN_AGE_DAYS + 1) * 86400)
    made = []
    for i in range(max(1, min(count, 20))):
        name = f"demo_{ts}_{i}.log"
        with open(os.path.join(LOG_DIR, name), "wb") as f:
            f.write(b"x" * 10_000)
        made.append(name)
    log.warning("DISK created %s redundant demo log file(s)", len(made))
    st = disk_log_once(note="after new files")
    return {"created": made, "usage_percent": st["usage_percent"]}


# --------------------------------------------------------------------------
if __name__ == "__main__":
    if "--init-db" in sys.argv:
        init_db()
        sys.exit(0)
    uvicorn.run(app, host=HOST, port=PORT, log_level="info")
