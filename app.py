"""
Retail Banking demo app - single file.

Contains:
  1. DB table creation + seed script   (runs on startup, or: python retail_bank.py --init-db)
  2. Main server (FastAPI + uvicorn)
  3. Two business APIs
       GET  /accounts/{account_number}   -> balance enquiry
       POST /transactions                -> deposit / withdraw (updates balance)
  4. A "flaky" API: after FAIL_AFTER_SECONDS the chosen API is shut down and
     returns 503 until restarted via  POST /admin/restart/{api_name}
     (that restart hook is what your future auto-restart monitor will call)
  5. Periodic client: calls the GET and POST APIs every CLIENT_INTERVAL_SECONDS
     so logs are generated continuously (successes AND failures).

Run:  uvicorn app:app --reload   (or: python app.py)
"""
import logging
import os
import random
import sys
import threading
import time
from contextlib import asynccontextmanager
from urllib.parse import urlparse
from logging.handlers import RotatingFileHandler

import requests
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
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
LOG_MAX_BYTES = int(os.getenv("LOG_MAX_BYTES", "100000"))   # rotate every ~100 KB
LOG_BACKUP_COUNT = int(os.getenv("LOG_BACKUP_COUNT", "4"))  # keep 4 old versions, delete older
CLIENT_BASE_URL = os.getenv("CLIENT_BASE_URL", f"http://127.0.0.1:{PORT}")

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
    log.info("DB ready (via Neon HTTP): table 'accounts' exists and seed data present")


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
# App + APIs
# --------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    flaky.start()
    if CLIENT_ENABLED:
        threading.Thread(target=client_loop, daemon=True).start()
    yield


app = FastAPI(title="Retail Banking", lifespan=lifespan)


class Transaction(BaseModel):
    account_number: str
    type: str = Field(pattern="^(deposit|withdraw)$")
    amount: float = Field(gt=0)


@app.get("/health")
def health():
    return {
        "status": "ok",
        "apis": {
            "get_account": flaky.is_up("get_account"),
            "post_transactions": flaky.is_up("post_transactions"),
        },
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
def restart_api(api_name: str):
    """Hook for your future auto-restart monitor."""
    if api_name != flaky.api_name:
        raise HTTPException(404, f"'{api_name}' is not the managed flaky API")
    flaky.start()
    log.warning("API '%s' RESTARTED via admin endpoint", api_name)
    return {"restarted": api_name, "auto_stop_in_seconds": FAIL_AFTER_SECONDS}


# --------------------------------------------------------------------------
if __name__ == "__main__":
    if "--init-db" in sys.argv:
        init_db()
        sys.exit(0)
    uvicorn.run(app, host=HOST, port=PORT, log_level="info")