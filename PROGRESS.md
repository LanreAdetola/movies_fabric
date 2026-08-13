# TMDb Medallion Project — Progress Checklist

Live status tracker. Design rationale lives in [CLAUDE.md](CLAUDE.md); this file is just
*what's done*. Update it at the end of every session before context is lost.

**Workspace:** `movies` (id `d78dd0c7-584e-417f-8893-4f4dd090b375`)
**Lakehouse:** `movies_lh` — schema-enabled, default schema `dbo`
**Notebooks:** `data_ingest`, `data_transform`
**Git:** `LanreAdetola/movies_fabric` @ `main` — connected and committing
**Last updated:** 2026-08-13

---

## Section 0 — Choose the API ✅ COMPLETE

- [x] API chosen: **TMDb** (`https://api.themoviedb.org/3`)
- [x] Screened against all four criteria — nested ✅, pagination ✅, incremental filter ✅, natural key ✅
- [x] Contract recorded in CLAUDE.md §1 (base URL, auth, pagination, natural key, watermark, rate limit)
- [x] v3 API key obtained, stored locally in `.env` (gitignored)
- [ ] **Open decision:** filter the global changes feed to a seeded population, or ingest everything?
      (CLAUDE.md §6 item 2 — recommendation is to filter for the first build)

## Section 1 — Setup 🔶 IN PROGRESS

- [x] Fabric workspace created — `movies`
- [x] **Connect workspace to Git** — `LanreAdetola/movies_fabric`, branch `main`.
      Hit `Git_GitProviderCredentialsNotAuthorizedError` on first commit; cause was the PAT scope,
      not the Fabric/GitHub email mismatch (which is normal and irrelevant). Fixed with a classic
      PAT carrying `repo`. First sync commit landed 2026-08-13 14:47Z, 3 items.
- [x] Clone repo locally → `~/Desktop/fabric_project_builder/movies_fabric`, in sync with `origin/main`
- [ ] **Attach `movies_lh` as default lakehouse on `data_transform`** — currently
      `"dependencies": {}`, so silver will fail with a misleading "table not found".
      `data_ingest` is correctly attached (lakehouse id `60548a93-566e-4621-93fd-71406dc7ae2a`)
- [x] Design doc `CLAUDE.md` written
- [x] Lakehouse created — **`movies_lh`**, **schema-enabled confirmed**
      (`lakehouse.metadata.json` → `{"defaultSchema":"dbo"}`)
- [x] Notebook `data_ingest` created
- [x] Notebook `data_transform` created
- [x] Key Vault created — `tmdb-api-key`, rg `fabric-api-rg`, spaincentral, RBAC auth enabled
      → URI `https://tmdb-api-key.vault.azure.net/`
- [x] Granted *Key Vault Secrets Officer* to `lanre@futureanalytics.be` on the vault
      (learned: Owner is control-plane only — does NOT grant data-plane access under RBAC auth)
- [x] Secret `tmdb-api-key` created, enabled, no expiry, content type `text`
- [x] **Verified**: vault value matches `.env`, and a live TMDb call using the vault value → HTTP 200
- [ ] Confirm the same call works from inside a Fabric notebook (runs as submitting user — same
      identity, so it should; verify anyway before building on it)
- [ ] Create Fabric Environment (only if libraries beyond runtime default are needed)
- [ ] Note capacity type (trial F64 / paid SKU) — drives the concurrency errors in §7

## Section 2 — Bronze 🔶 CODE WRITTEN, NOT YET RUN

`data_ingest.Notebook/notebook-content.py` — 9 cells, syntax-checked locally. Not executed in
Fabric yet, so every box below is *written* but not *proven*.

- [x] Land raw JSON to `Files/bronze/movie_changes/` and `Files/bronze/movie_details/`
- [x] Real pagination on `/movie/changes` (loop to `total_pages`) with `max_pages` cap
- [x] Detail fan-out loop with `max_detail_calls` cap and ~10 req/s throttle
- [x] `redact(url)` helper — strips `api_key` before any URL is logged or stored
- [x] Add `_ingested_at`, `_source` (redacted), `_batch_id` — stamped before landing
- [x] try/except per call, `raise_for_status()`, log-and-skip, honour 429 + `Retry-After`
- [x] Append to `bronze.movie_changes_raw` + `bronze.movie_details_raw` with `mergeSchema`
- [x] Print row counts at every stage
- [x] Watermark read at start, written only after verified load
- [x] `full_reload` + `filter_to_known_ids` in a **parameter cell**
- [ ] **Run it in Fabric** (first run auto-takes the seed path — no watermark exists)

**Acceptance:**
- [ ] `records_written == records_fetched`
- [ ] Second run grows count by exactly the new records (proves append)
- [ ] `_batch_id` distinct count == number of runs
- [ ] `printSchema()` still nested
- [ ] Changed-ID count reconciles against detail calls attempted + skipped
- [ ] **No API key present anywhere in the table** — grep `_source` to confirm

## Section 3 — Silver ⬜ NOT STARTED

- [ ] Flatten `belongs_to_collection` struct via dot notation
- [ ] Explode `genres`, `production_companies`, `credits.cast`, `credits.crew` into child tables
- [ ] Dedup on `id` via `row_number()` over `_ingested_at desc`
- [ ] Explicit `StructType` / casts — no inferred types
- [ ] Handle `release_date` empty-string → null, log the count lost
- [ ] `MERGE INTO` on all tables
- [ ] **Child tables: `whenNotMatchedBySourceDelete()` scoped to batch movie_ids** (CLAUDE.md §3)

**Acceptance:**
- [ ] `silver.movies.count() == bronze.movie_details_raw.select("id").distinct().count()`
- [ ] No duplicate `id` in `silver.movies`
- [ ] Re-running twice changes nothing (idempotence)
- [ ] Child rows ≥ parent count; every child `movie_id` exists in parent
- [ ] No nulls in `id` / `_ingested_at`
- [ ] Removed-genre test: delete a genre in bronze, rerun, child row disappears

## Section 4 — Gold ⬜ NOT STARTED

- [ ] Business question written down: *which genres deliver best ROI, by release decade*
- [ ] Build `gold.genre_decade_performance` at grain (`genre_id`, `release_decade`)
- [ ] INNER join movies × movie_genres, justified in notes
- [ ] Filter `budget > 0 AND revenue > 0`, log excluded count
- [ ] Document the multi-genre fan-out on the report page
- [ ] Direct Lake semantic model on gold + silver detail tables
- [ ] Cross-filter direction **Both** where cross-page filtering is needed

**Acceptance:**
- [ ] Grain unique
- [ ] Hand-reconcile one cell (e.g. Action/1990s) against silver
- [ ] Report visuals pull pre-aggregated numbers from gold

## Section 5 — Incremental loading ⬜ NOT STARTED

- [ ] `control._pipeline_watermark` Delta table (entity, last_value, updated_at)
- [ ] Read watermark → `start_date`, write only after successful load
- [ ] **Chunk windows to ≤14 days** (API hard cap — CLAUDE.md §4 step 3)
- [ ] 1-day overlap on the window
- [ ] `full_reload` flag → `/discover/movie` seed path, in a parameter cell
- [ ] Document what "late-arriving" means here (no source-side `updated_at`)

**Acceptance:**
- [ ] Run twice back-to-back → second fetches ~0, silver/gold counts unchanged

## Section 6 — Orchestration ⬜ NOT STARTED

- [ ] Pipeline: two Run-notebook activities, success dependency
- [ ] Parameters `run_date`, `page_size`, `full_reload` → notebook parameter cell
- [ ] Schedule + on-failure path (Teams/Outlook/Activator)
- [ ] Verify parameters arrive — log them in cell 1, check run snapshot

## Section 7 — Monitoring ⬜ NOT STARTED

- [ ] Read run history in Monitoring Hub, open a notebook snapshot
- [ ] Induce and diagnose 4 failures: bad URL (4xx), 429, schema drift, type-mismatch write
- [ ] Row counts logged in/out per stage
- [ ] Know the `[TooManyRequestsForCapacity]` / HTTP 430 fix (cancel stuck session)

## Section 8 — Security & governance ⬜ NOT STARTED

- [ ] Sensitivity label on Lakehouse + report
- [ ] RLS/CLS via Manage OneLake security
- [ ] **Test as a Viewer** — Admin/Member/Contributor bypass OneLake security
- [ ] SQL analytics endpoint → user's identity mode
- [ ] Remove test users from DefaultReader
- [ ] Note: needs a second identity. If unavailable, mark configure-only

## Section 9 — Performance ⬜ NOT STARTED

- [ ] `OPTIMIZE` bronze after several runs; compare file count + query time
- [ ] Understand V-Order and `VACUUM` 7-day retention
- [ ] Read Spark UI plan for slowest gold query, find the shuffle
- [ ] Note one change for 100× data

## Section 10 — Version control & CI/CD ⬜ NOT STARTED

- [ ] Commit after each layer, messages describing decisions
- [ ] Branch to feature workspace, sync back, resolve one conflict deliberately
- [ ] Deployment pipeline dev → test, note what needs rebinding

---

## Session log

| Date | Session | Outcome |
|---|---|---|
| 2026-08-13 | 1 | API screened and chosen (TMDb); design doc written; workspace `movies` created; auth trap found (v3 key in URL → must redact `_source`) |

## Not covered by this project (study separately)

Real-Time Intelligence (Eventstream/Eventhouse/KQL/Activator) · Warehouse & T-SQL ·
Dataflows Gen2 / Copy job · Mirroring & shortcuts · Spark structured streaming
