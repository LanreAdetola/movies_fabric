# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "60548a93-566e-4621-93fd-71406dc7ae2a",
# META       "default_lakehouse_name": "movies_lh",
# META       "default_lakehouse_workspace_id": "d78dd0c7-584e-417f-8893-4f4dd090b375",
# META       "known_lakehouses": [
# META         {
# META           "id": "60548a93-566e-4621-93fd-71406dc7ae2a"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # data_ingest — bronze layer
# TMDb → `Files/bronze/` → `bronze.movie_changes_raw` + `bronze.movie_details_raw`
# Two-stage ingestion: `/movie/changes` yields IDs only, so we fan out to `/movie/{id}` for the
# actual payload. Bronze is an append-only log — re-runs add history, dedup happens in silver.

# PARAMETERS CELL ********************

# Pipeline parameters. Fabric overrides these via notebook base parameters.
full_reload           = False   # True = ignore watermark, seed from /discover/movie
page_size             = 100     # informational: /movie/changes is fixed at 100/page
max_pages             = 50      # hard cap per changes window — stops a bug eating the capacity
max_detail_calls      = 500     # hard cap on the fan-out — the real cost driver
seed_pages            = 10      # /discover/movie pages when seeding (20 movies/page)
filter_to_known_ids   = True    # ignore changed movies not already in our population
lookback_hours        = 24      # overlap subtracted from the watermark
run_date              = ""      # ISO date; blank = today

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json, os, re, time, uuid
from datetime import datetime, timedelta, timezone

import requests
from pyspark.sql import functions as F

BASE_URL      = "https://api.themoviedb.org/3"
VAULT_URL     = "https://tmdb-api-key.vault.azure.net/"
SECRET_NAME   = "tmdb-api-key"
MAX_WINDOW_DAYS = 14            # TMDb hard limit on /movie/changes
THROTTLE_SEC  = 0.1             # ~10 req/s, well under TMDb's ~40-50/s soft ceiling

BATCH_ID = str(uuid.uuid4())
RUN_TS   = datetime.now(timezone.utc)
END_DATE = datetime.fromisoformat(run_date).date() if run_date else RUN_TS.date()

LAND_ROOT = "/lakehouse/default/Files/bronze"

print(f"batch_id : {BATCH_ID}")
print(f"run ts   : {RUN_TS.isoformat()}")
print(f"end_date : {END_DATE}")
print(f"mode     : {'FULL RELOAD (seed)' if full_reload else 'incremental'}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- auth + HTTP -------------------------------------------------------------------------
# The v3 API key travels as a QUERY PARAMETER, so every request URL contains the secret.
# redact() must be applied anywhere a URL is printed, logged, or written to a table --
# especially inside exception handlers, which is where it leaks first.

API_KEY = notebookutils.credentials.getSecret(VAULT_URL, SECRET_NAME)

SESSION = requests.Session()

def redact(url: str) -> str:
    return re.sub(r"(api_key=)[^&]*", r"\1<redacted>", url or "")

def get_json(path: str, params: dict = None, max_retries: int = 5):
    """Returns (payload, error). Never raises -- callers log and skip."""
    params = dict(params or {})
    params["api_key"] = API_KEY
    url = f"{BASE_URL}{path}"

    for attempt in range(max_retries):
        try:
            r = SESSION.get(url, params=params, timeout=30)
            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", 2 ** attempt))
                print(f"  429 -> sleeping {wait}s ({redact(r.url)})")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json(), None
        except Exception as e:
            if attempt == max_retries - 1:
                return None, f"{type(e).__name__}: {e}"
            time.sleep(2 ** attempt)
    return None, "max retries exceeded"

def source_url(path: str, params: dict = None) -> str:
    """Redacted URL for the _source audit column."""
    qs = "&".join(f"{k}={v}" for k, v in (params or {}).items())
    return redact(f"{BASE_URL}{path}?{qs}&api_key=x")

# smoke test
_probe, _err = get_json("/movie/550")
print("auth ok:", _err is None, "|", _probe.get("title") if _probe else _err)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- schemas + watermark control table ---------------------------------------------------
# NOTE: the checklist calls this table `_pipeline_watermark`. We drop the leading underscore.
# Spark treats `_`-prefixed paths as hidden/metadata and skips them when scanning directories,
# which makes leading-underscore table names a genuine source of "table exists but reads empty".

spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS control")

spark.sql("""
CREATE TABLE IF NOT EXISTS control.pipeline_watermark (
    entity     STRING,
    last_value STRING,
    updated_at TIMESTAMP
) USING DELTA
""")

def read_watermark(entity: str):
    row = (spark.table("control.pipeline_watermark")
                .filter(F.col("entity") == entity)
                .orderBy(F.col("updated_at").desc())
                .limit(1)
                .collect())          # bounded to 1 row by construction
    return row[0]["last_value"] if row else None

def write_watermark(entity: str, value: str):
    spark.sql(f"DELETE FROM control.pipeline_watermark WHERE entity = '{entity}'")
    (spark.createDataFrame([(entity, value, RUN_TS)],
                           "entity string, last_value string, updated_at timestamp")
          .write.mode("append").saveAsTable("control.pipeline_watermark"))

wm = read_watermark("movie_changes")
print(f"watermark: {wm or '(none — first run)'}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- stage 1: collect candidate movie IDs ------------------------------------------------

def date_windows(start, end, max_days=MAX_WINDOW_DAYS):
    """TMDb rejects ranges > 14 days. A run that hasn't fired in weeks must chunk, not fail."""
    out, cur = [], start
    while cur < end:
        nxt = min(cur + timedelta(days=max_days), end)
        out.append((cur, nxt))
        cur = nxt
    return out or [(start, end)]

changed_ids, pages_fetched, page_errors = set(), 0, []

if full_reload or wm is None:
    reason = "full_reload flag" if full_reload else "no watermark — first run"
    print(f"SEED path ({reason}): /discover/movie, {seed_pages} pages")
    for page in range(1, seed_pages + 1):
        params = {"sort_by": "popularity.desc", "page": page}
        data, err = get_json("/discover/movie", params)
        pages_fetched += 1
        if err:
            page_errors.append((f"discover p{page}", err)); continue
        changed_ids.update(m["id"] for m in data.get("results", []))
        if page >= data.get("total_pages", 0):
            break
        time.sleep(THROTTLE_SEC)
else:
    # /movie/changes is date-granular, so the overlap rounds up to whole days
    overlap_days = max(1, -(-lookback_hours // 24))
    start_date = datetime.fromisoformat(wm).date() - timedelta(days=overlap_days)
    wins = date_windows(start_date, END_DATE)
    print(f"INCREMENTAL path: {start_date} -> {END_DATE} in {len(wins)} window(s)")

    for ws, we in wins:
        page = 1
        while page <= max_pages:
            params = {"start_date": ws.isoformat(), "end_date": we.isoformat(), "page": page}
            data, err = get_json("/movie/changes", params)
            pages_fetched += 1
            if err:
                page_errors.append((f"changes {ws}..{we} p{page}", err)); break
            changed_ids.update(m["id"] for m in data.get("results", []))
            total = data.get("total_pages", 1)
            if page >= total:
                break
            page += 1
            time.sleep(THROTTLE_SEC)
        else:
            print(f"  WARNING: hit max_pages={max_pages} for {ws}..{we} — window truncated")

print(f"pages fetched : {pages_fetched}")
print(f"page errors   : {len(page_errors)}")
for label, e in page_errors[:5]:
    print(f"   {label}: {e}")
print(f"candidate ids : {len(changed_ids)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- narrow the candidate set ------------------------------------------------------------
# The changes feed is GLOBAL: it returns every movie TMDb touched, most of which we've never
# seen. Filtering to our existing population keeps run times predictable. Flip the flag to let
# the population grow organically.

candidates = sorted(changed_ids)

if filter_to_known_ids and not full_reload and spark.catalog.tableExists("bronze.movie_details_raw"):
    cand_df = spark.createDataFrame([(i,) for i in candidates], "id long")
    known   = spark.table("bronze.movie_details_raw").select("id").distinct()
    kept    = (cand_df.join(known, "id", "inner")
                      .limit(max_detail_calls)
                      .collect())          # bounded by max_detail_calls, not by table size
    candidates = [r["id"] for r in kept]
    print(f"filtered to known population: {len(changed_ids)} -> {len(candidates)}")

if len(candidates) > max_detail_calls:
    print(f"WARNING: capping {len(candidates)} -> {max_detail_calls} detail calls")
    candidates = candidates[:max_detail_calls]

print(f"detail calls to make: {len(candidates)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- stage 2: fan out to /movie/{id} and land RAW json ------------------------------------
# Audit columns are stamped onto each record BEFORE landing, so the raw files are already
# traceable. Nothing is flattened here -- bronze preserves the source shape.

CHANGES_DIR = f"{LAND_ROOT}/movie_changes/{BATCH_ID}"
DETAILS_DIR = f"{LAND_ROOT}/movie_details/{BATCH_ID}"
os.makedirs(CHANGES_DIR, exist_ok=True)
os.makedirs(DETAILS_DIR, exist_ok=True)

INGESTED_AT = RUN_TS.isoformat()

# land the candidate id list (bronze.movie_changes_raw)
with open(f"{CHANGES_DIR}/part-0000.jsonl", "w") as fh:
    for mid in sorted(changed_ids):
        fh.write(json.dumps({
            "id": mid,
            "_ingested_at": INGESTED_AT,
            "_source": source_url("/movie/changes"),
            "_batch_id": BATCH_ID,
        }) + "\n")

# fan out
records_fetched, detail_errors, chunk, chunk_no = 0, [], [], 0
CHUNK = 200

def flush(buf, n):
    if not buf:
        return
    with open(f"{DETAILS_DIR}/part-{n:04d}.jsonl", "w") as fh:
        for rec in buf:
            fh.write(json.dumps(rec) + "\n")

for i, mid in enumerate(candidates, 1):
    params = {"append_to_response": "credits,keywords"}
    data, err = get_json(f"/movie/{mid}", params)
    if err:
        detail_errors.append((mid, err))
    else:
        data["_ingested_at"] = INGESTED_AT
        data["_source"]      = source_url(f"/movie/{mid}", params)
        data["_batch_id"]    = BATCH_ID
        chunk.append(data)
        records_fetched += 1

    if len(chunk) >= CHUNK:
        flush(chunk, chunk_no); chunk_no += 1; chunk = []
    if i % 100 == 0:
        print(f"  {i}/{len(candidates)} ...")
    time.sleep(THROTTLE_SEC)

flush(chunk, chunk_no)

print(f"records fetched : {records_fetched}")
print(f"detail errors   : {len(detail_errors)}")
for mid, e in detail_errors[:5]:
    print(f"   movie {mid}: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- load landed json -> delta (append + mergeSchema) -------------------------------------
# mergeSchema matters because TMDb adds fields over time; bronze must absorb drift, not reject it.

changes_written = 0
details_written = 0

changes_df = spark.read.json(f"Files/bronze/movie_changes/{BATCH_ID}")
changes_written = changes_df.count()
(changes_df.write.mode("append").option("mergeSchema", "true")
           .saveAsTable("bronze.movie_changes_raw"))

if records_fetched > 0:
    details_df = spark.read.json(f"Files/bronze/movie_details/{BATCH_ID}")
    details_written = details_df.count()
    (details_df.write.mode("append").option("mergeSchema", "true")
               .saveAsTable("bronze.movie_details_raw"))
else:
    print("no detail records this run — skipping details write")

print(f"changes written : {changes_written}")
print(f"details written : {details_written}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- advance the watermark ONLY on success ------------------------------------------------
# If this cell never runs, the next run re-requests the same window. Bronze gains duplicates
# (fine -- it's an append log) and silver's dedup+MERGE collapses them. That is the whole
# argument for MERGE in silver.

load_ok = (details_written == records_fetched) and (records_fetched > 0 or full_reload is False)

if load_ok:
    write_watermark("movie_changes", END_DATE.isoformat())
    print(f"watermark advanced -> {END_DATE.isoformat()}")
else:
    print("watermark NOT advanced — load did not verify clean")

print("\n" + "=" * 60)
print(f"batch_id        : {BATCH_ID}")
print(f"pages fetched   : {pages_fetched}")
print(f"page errors     : {len(page_errors)}")
print(f"candidate ids   : {len(changed_ids)}")
print(f"detail calls    : {len(candidates)}")
print(f"records fetched : {records_fetched}")
print(f"detail errors   : {len(detail_errors)}")
print(f"changes written : {changes_written}")
print(f"details written : {details_written}")
print("=" * 60)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
