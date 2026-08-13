# TMDb Medallion Project — Design Doc

DP-700 practice project. TMDb REST API → schema-enabled Fabric Lakehouse (bronze/silver/gold)
→ Direct Lake semantic model → report.

**Environment:** workspace `movies` → Lakehouse **`movies_lh`** (schema-enabled, default schema
`dbo`), notebooks **`data_ingest`** and **`data_transform`**. Workspace is connected to Git at
`LanreAdetola/movies_fabric` @ `main`, so notebooks live in the repo as
`<name>.Notebook/notebook-content.py` and can be edited as files.

**Working mode:** author locally, then either commit and pull into Fabric via Git, or paste into
the web UI. Mark cell boundaries explicitly either way — Fabric's format uses `# CELL ********`
separators in `notebook-content.py`.

---

## 1. API contract

| Item | Value |
|---|---|
| Base URL | `https://api.themoviedb.org/3` |
| Auth | **v3 API key** as `?api_key=<key>` query param (this is the key we hold) |
| Secret | `notebookutils.credentials.getSecret('https://tmdb-api-key.vault.azure.net/', 'tmdb-api-key')` |
| Vault | `tmdb-api-key` (rg `fabric-api-rg`, sub *Azure for Students*, spaincentral, **RBAC auth enabled**) |
| Local dev | `.env` → `API_Key=...`, gitignored. Never transfers to Fabric — Key Vault only there |

| Pagination | `?page=N`; response has `page`, `total_pages`, `total_results`. Most list endpoints cap at page 500 |
| Incremental filter | `/movie/changes?start_date=&end_date=` — **max 14-day window** |
| Natural key | `id` (TMDb movie ID, integer, stable) |
| Watermark | our own `end_date` from the last successful run — **not a field in the payload** |
| Rate limit | soft, ~40–50 req/sec. Old 40-per-10s limit removed Dec 2019 |

> **Auth trap — the key travels in the URL.** Because v3 auth is a query parameter, the full
> request URL contains the secret. The design puts the request URL in bronze's `_source` audit
> column, which would persist the API key into a Delta table in plain text, forever, where it also
> reaches the SQL endpoint and any report built on it. **`_source` must store the URL with
> `api_key` stripped or masked** before it is written. `redact(url)` in `data_ingest` does this;
> use it everywhere a URL is logged or stored — including inside exception handlers, which is
> where it will otherwise leak first.
>
> Optional cleanup: generate a v4 read access token in TMDb settings and use
> `Authorization: Bearer <token>` instead, which keeps the secret out of the URL entirely. Worth
> doing if you want the more production-shaped pattern; not required.

**Endpoints used:**
- `/movie/changes?start_date=&end_date=&page=` — incremental ID feed. Returns `{id, adult}` only, 100/page
- `/movie/{id}?append_to_response=credits,keywords` — the actual payload
- `/discover/movie?sort_by=popularity.desc&page=` — seed load only (20/page)

---

## 2. Response shape

From `/movie/{id}?append_to_response=credits,keywords`:

**Scalars:** `adult`, `backdrop_path`, `budget`, `homepage`, `id`, `imdb_id`, `original_language`,
`original_title`, `overview`, `popularity`, `poster_path`, `release_date`, `revenue`, `runtime`,
`status`, `tagline`, `title`, `video`, `vote_average`, `vote_count`

**Struct (nullable):**
- `belongs_to_collection` → `{id, name, poster_path, backdrop_path}`. Null for standalone films —
  the majority. Flatten via dot notation into `collection_id` / `collection_name`.

**Variable-length arrays of structs:**

| Array | Element | Typical length | Decision |
|---|---|---|---|
| `genres` | `{id, name}` | 1–4, occasionally 0 | **Normalize** → `silver.movie_genres` |
| `production_companies` | `{id, logo_path, name, origin_country}` | 0–15 | **Normalize** → `silver.movie_companies` |
| `production_countries` | `{iso_3166_1, name}` | 0–5 | Normalize (optional; low value) |
| `spoken_languages` | `{english_name, iso_639_1, name}` | 0–5 | Normalize (optional; low value) |
| `credits.cast` | `{credit_id, id, name, character, order, ...}` | 10–150+ | **Normalize** → `silver.movie_cast` |
| `credits.crew` | `{credit_id, id, name, job, department, ...}` | 10–500+ | **Normalize** → `silver.movie_crew` |
| `keywords.keywords` | `{id, name}` | 0–30 | Normalize (optional) |

**Array of scalars:** `origin_country` — array of ISO strings.

**Fixed-size arrays: none.** Every array here is variable-length, so nothing should be pivoted to
columns. If you want the pivot exercise the checklist mentions, the only defensible candidate is
`genres` → ~19 boolean flag columns, and it is the wrong call here: it hardcodes the genre list
into the schema and breaks when TMDb adds one. Normalize instead.

---

## 3. Table inventory

**Landing:** `Files/bronze/movie_changes/<batch_id>/`, `Files/bronze/movie_details/<batch_id>/`
— raw JSON exactly as returned, before any parsing.

### Bronze — append + mergeSchema, immutable log

| Table | Grain | Notes |
|---|---|---|
| `bronze.movie_changes_raw` | one row per (`_batch_id`, `movie_id`) | thin: id, adult, audit cols |
| `bronze.movie_details_raw` | one row per (`_batch_id`, `movie_id`) | full nested payload preserved |

Audit columns on every bronze row: `_ingested_at` (timestamp), `_source` (endpoint URL),
`_batch_id` (uuid per notebook run).

### Silver — MERGE, deduplicated, explicit types

| Table | Grain | Merge key |
|---|---|---|
| `silver.movies` | one row per `movie_id` | `id` |
| `silver.movie_genres` | one row per (`movie_id`, `genre_id`) | composite |
| `silver.movie_companies` | one row per (`movie_id`, `company_id`) | composite |
| `silver.movie_cast` | one row per (`movie_id`, `credit_id`) | composite |
| `silver.movie_crew` | one row per (`movie_id`, `credit_id`) | composite |

Dedup: `row_number()` over `partitionBy(id)` ordered by `_ingested_at desc`, keep rank 1. Bronze
holds every run's copy of a movie, so this is mandatory, not optional.

**Child-table subtlety — do not miss this.** A plain
`.whenMatchedUpdateAll().whenNotMatchedInsertAll()` on a child table inserts new children but
never removes ones that disappeared upstream. If a movie drops a genre, the stale row survives
forever and silently inflates every gold aggregate. Correct pattern: scope the merge to the
movie_ids present in this batch and add `whenNotMatchedBySourceDelete()` with that same scope, so
the child set is replaced per parent rather than accumulated.

### Gold — overwrite

**Business question:** *Which genres deliver the best return on production budget, and how has
that shifted by release decade?*

| Table | Grain | Write mode |
|---|---|---|
| `gold.genre_decade_performance` | one row per (`genre_id`, `release_decade`) | overwrite |

Measures: `movie_count`, `total_budget`, `total_revenue`, `roi` (= revenue/budget),
`avg_vote_average`, `avg_popularity`.

Joins: `silver.movies` INNER JOIN `silver.movie_genres` on `movie_id`.
- **INNER, deliberately** — a movie with no genre cannot be attributed to any genre bucket, so it
  is uncategorizable and correctly excluded.
- **Fan-out is expected and intended.** A film tagged Action + Sci-Fi contributes its full budget
  and revenue to *both* rows. That means `SUM(total_budget)` across all gold rows exceeds true
  total industry budget. This is correct for "per-genre performance" and must be documented on the
  report page, or a reviewer will read it as a bug.
- Filter `budget > 0 AND revenue > 0` before computing ROI — a large share of TMDb records carry
  placeholder zeros, and including them makes ROI meaningless. Log the excluded count.

Semantic model: Direct Lake over `gold.genre_decade_performance` + `silver.movies` (row-level
drill-through) + `silver.movie_genres`.

---

## 4. Incremental strategy

**Control table:** `control.pipeline_watermark` — Delta, columns `entity`, `last_value`,
`updated_at`. A table, not a notebook variable, so it survives session death.

> Deviation from the checklist, deliberate: it names this table `_pipeline_watermark`. We dropped
> the leading underscore because Spark treats `_`-prefixed paths as hidden/metadata and skips them
> when scanning directories — a genuine source of "the table exists but reads back empty".

**Run sequence:**

1. Read `last_value` for `entity='movie_changes'` (an ISO date).
2. `start_date = last_value - 1 day` (deliberate overlap so boundary records aren't skipped),
   `end_date = today`.
3. **Chunk the window into ≤14-day slices.** The API rejects wider ranges, so a run that hasn't
   fired in three weeks must loop over multiple windows rather than fail. This is the single most
   likely thing to break in production.
4. Per window: paginate `/movie/changes` until `page == total_pages`, with `MAX_PAGES` hard cap.
5. Union and distinct the collected movie IDs; land raw to Files.
6. **Fan out:** `GET /movie/{id}?append_to_response=credits,keywords` per ID, with a
   `MAX_DETAIL_CALLS` hard cap and throttling to ~10 req/s (well under the soft limit). try/except
   per call — log and skip, don't kill the run. Honour `429` / `Retry-After` with backoff.
7. Land raw JSON → append to `bronze.movie_details_raw` with `mergeSchema`.
8. **Only after a successful write**, update the watermark to `end_date`.

**`full_reload` flag** (parameter cell): bypasses the watermark entirely and runs the seed path —
`/discover/movie?sort_by=popularity.desc` for N pages, then the same fan-out. Needed because the
changes feed alone never establishes a starting population.

**On failure:** the watermark is not advanced, so the next run re-requests the same window. Bronze
gains duplicate copies (fine — it's an append log). Silver's dedup-then-MERGE collapses them.
Net effect: reruns are safe. This is the entire justification for MERGE + dedup in silver.

**What "late-arriving" means here:** TMDb's payload has **no `updated_at` field**. Nothing in the
data tells you when a movie changed — the only signal is that its ID appeared in the changes feed.
So the watermark is a *collection-time* watermark, not a source-time one, and the 1-day overlap is
the only protection against a record landing on a window boundary.

---

## 5. Acceptance checks

**Bronze**
- `records_written == records_fetched` for the run
- second run grows `df.count()` by exactly the new record count (proves append, not overwrite)
- `select("_batch_id").distinct().count()` equals the number of runs
- `printSchema()` still shows `genres`, `credits` etc. as nested — if flat, something flattened too early
- changed-ID count reconciles: `bronze.movie_changes_raw` distinct ids for the batch == detail calls attempted + skipped

**Silver**
- `silver.movies.count() == bronze.movie_details_raw.select("id").distinct().count()`
- `silver.movies.groupBy("id").count().filter("count > 1").count() == 0`
- **re-running the whole notebook twice changes nothing** — the real test of MERGE
- every child table: row count ≥ parent count, and every `movie_id` in a child exists in `silver.movies`
- no nulls in `id` or `_ingested_at`
- drop a genre from one movie's bronze row by hand, rerun → the child row disappears (proves the
  `whenNotMatchedBySourceDelete` scoping actually works)

**Gold**
- `groupBy("genre_id", "release_decade").count().filter("count>1").count() == 0`
- hand-reconcile one cell: pick Action/1990s, count matching silver rows manually, compare
- excluded zero-budget count is logged and plausible
- fan-out documented on the report page

**Incremental**
- run twice back-to-back: second run fetches ~0 changed IDs, silver and gold row counts unchanged

---

## 6. What will make this harder than expected

1. **Two-stage ingestion.** `/movie/changes` returns IDs only, so the run length is driven by
   *number of changed movies*, not by a page cap. A busy window is thousands of detail calls. Both
   caps (`MAX_PAGES` and `MAX_DETAIL_CALLS`) matter, and they bound different things.
2. **The changes feed is global.** It returns IDs for every movie TMDb touched, including
   thousands you never seeded. Decide explicitly: (a) ingest them all and let the population grow
   organically, or (b) inner-join against your seeded ID set. **Recommendation: (b) for the first
   build** — it keeps run times predictable while you're still debugging. Revisit once bronze is stable.
3. **The 14-day cap creates permanent gaps** if a failed run sits unattended. The chunking loop in
   step 3 is not optional polish.
4. **`release_date` is an empty string, not null**, for unreleased films. `to_date("")` yields null
   silently — cast deliberately and count what you lose.
5. **Zero budget/revenue is rampant**, and it's a placeholder, not a real zero. Any ROI calculation
   that doesn't filter these is wrong.
6. **`credits.crew` is the largest table, not `cast`** — verified against `/movie/550`
   (Fight Club): 76 cast, **188 crew**, 14 keywords, 5 production companies, 2 genres. So one
   movie yields ~264 credit rows. At 5,000 movies that's **~1.3M rows in the credit tables alone**,
   several times the size of every other table combined. This is where `.collect()` /
   `.toPandas()` will kill you, and where `OPTIMIZE` in Section 9 shows a measurable difference.
   If run times become painful while iterating, drop `crew` from `append_to_response` first — it's
   the cheapest large win and gold doesn't depend on it.
7. **No source-side timestamp** (see section 4) — the checklist assumes an `updated_at`-style
   field and TMDb has none. Say so in your notes rather than pretending the watermark is source-derived.

---

## 7. Current state

**Live status lives in [PROGRESS.md](PROGRESS.md)** — single source of truth for what's built.
Don't duplicate it here; this file holds design rationale, that one holds progress.

**Decisions locked:** schema-enabled Lakehouse `movies_lh` (not table-name prefixes); notebooks
authored as files in Git, run in Fabric; `append_to_response=credits,keywords` on detail calls;
v3 API key auth with mandatory URL redaction before storage; watermark table without a leading
underscore.

**Resolved:** §6 item 2 is now the `filter_to_known_ids` parameter, defaulting to `True`. A seed
load is required on the first run regardless, so this stopped being a fork in the design and
became a flag.
