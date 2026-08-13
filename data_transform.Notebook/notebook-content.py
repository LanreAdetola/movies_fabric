# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "64cc823f-d507-4130-8899-0302c1dae06d",
# META       "default_lakehouse_name": "movies_lh",
# META       "default_lakehouse_workspace_id": "9727ed9e-8b3f-4be3-82fb-94cc0590786c",
# META       "known_lakehouses": [
# META         {
# META           "id": "64cc823f-d507-4130-8899-0302c1dae06d"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # data_transform — silver layer
# `bronze.movie_details_raw` → `silver.movies` + 4 normalized child tables.
# Dedup on the natural key, explicit types, MERGE (never overwrite).
# Re-running this notebook must change nothing — that is the acceptance test.

# PARAMETERS CELL ********************

drop_and_rebuild = False   # True = drop silver tables first (use when changing the schema)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

SILVER_TABLES = ["silver.movies", "silver.movie_genres", "silver.movie_companies",
                 "silver.movie_cast", "silver.movie_crew"]

if drop_and_rebuild:
    for t in SILVER_TABLES:
        spark.sql(f"DROP TABLE IF EXISTS {t}")
    print("dropped:", ", ".join(SILVER_TABLES))

bronze = spark.table("bronze.movie_details_raw")
print("bronze rows          :", bronze.count())
print("bronze distinct ids  :", bronze.select("id").distinct().count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- dedup bronze to one row per movie ----------------------------------------------------
# Bronze is an append log: every run adds another copy of each movie it fetched. Silver must
# collapse those to the newest. _ingested_at arrives as a STRING (we wrote ISO text into JSON),
# so we cast it to a real timestamp rather than relying on lexicographic ordering happening to
# match chronological ordering.

latest = (bronze
    .withColumn("_ingested_ts", F.to_timestamp("_ingested_at"))
    .withColumn("_rn", F.row_number().over(
        Window.partitionBy("id").orderBy(F.col("_ingested_ts").desc())))
    .filter(F.col("_rn") == 1)
    .drop("_rn"))

latest.cache()
print("deduped to           :", latest.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- reusable upsert ----------------------------------------------------------------------
# Delta MERGE raises if MULTIPLE source rows match one target row, so every source is deduped
# on its merge keys first. TMDb does occasionally repeat a credit_id within one movie.

def upsert(df, table, keys, delete_missing=False):
    """MERGE df into table on `keys`. delete_missing=True removes target rows absent from the
    source -- only safe because we reprocess ALL of bronze, so the source carries every movie's
    complete child set. If you ever switch to processing a single batch, this must be scoped to
    that batch's movie_ids or it will delete every other movie's children."""
    src = df.dropDuplicates(keys)
    n = src.count()

    if not spark.catalog.tableExists(table):
        src.write.format("delta").saveAsTable(table)
        print(f"{table:<26} created  {n:>7} rows")
        return

    cond = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    m = (DeltaTable.forName(spark, table).alias("t")
         .merge(src.alias("s"), cond)
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll())
    if delete_missing:
        m = m.whenNotMatchedBySourceDelete()
    m.execute()
    print(f"{table:<26} merged   {n:>7} source rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- silver.movies ------------------------------------------------------------------------
# release_date arrives as a STRING and is an EMPTY STRING (not null) for unreleased films.
# to_date("") returns null silently, so we count the loss deliberately instead of discovering
# it later as a hole in the gold aggregates.

blank_release = latest.filter(
    F.col("release_date").isNull() | (F.trim(F.col("release_date")) == "")).count()
print(f"movies with no release_date: {blank_release} of {latest.count()}")

movies = latest.select(
    F.col("id").cast("long").alias("movie_id"),
    F.col("title").cast("string"),
    F.col("original_title").cast("string"),
    F.col("original_language").cast("string"),
    F.col("overview").cast("string"),
    F.col("tagline").cast("string"),
    F.col("status").cast("string"),
    F.to_date(F.when(F.trim(F.col("release_date")) == "", None)
               .otherwise(F.trim(F.col("release_date"))), "yyyy-MM-dd").alias("release_date"),
    F.col("runtime").cast("int").alias("runtime_minutes"),
    F.col("budget").cast("long"),
    F.col("revenue").cast("long"),
    F.col("popularity").cast("double"),
    F.col("vote_average").cast("double"),
    F.col("vote_count").cast("long"),
    F.col("adult").cast("boolean"),
    F.col("video").cast("boolean"),
    F.col("softcore").cast("boolean"),          # undocumented TMDb field, arrived via mergeSchema
    F.col("imdb_id").cast("string"),
    F.col("homepage").cast("string"),
    F.col("belongs_to_collection.id").cast("long").alias("collection_id"),
    F.col("belongs_to_collection.name").cast("string").alias("collection_name"),
    F.col("origin_country").alias("origin_countries"),
    F.col("_ingested_ts").alias("_ingested_at"),
    F.col("_batch_id").cast("string"),
).withColumn("release_year", F.year("release_date")
).withColumn("release_decade", (F.floor(F.year("release_date") / 10) * 10).cast("int"))

upsert(movies, "silver.movies", ["movie_id"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- child tables: explode the variable-length arrays -------------------------------------
# explode_outer would keep movies with empty arrays as null-child rows; we use explode, which
# drops them, because a movie with no genres genuinely has no genre relationships to record.

base = latest.select("id", "genres", "production_companies", "credits", "_ingested_ts", "_batch_id")

genres = (base.select(F.col("id").cast("long").alias("movie_id"),
                      F.explode("genres").alias("g"),
                      "_ingested_ts", "_batch_id")
          .select("movie_id",
                  F.col("g.id").cast("long").alias("genre_id"),
                  F.col("g.name").cast("string").alias("genre_name"),
                  F.col("_ingested_ts").alias("_ingested_at"), "_batch_id"))

companies = (base.select(F.col("id").cast("long").alias("movie_id"),
                         F.explode("production_companies").alias("c"),
                         "_ingested_ts", "_batch_id")
             .select("movie_id",
                     F.col("c.id").cast("long").alias("company_id"),
                     F.col("c.name").cast("string").alias("company_name"),
                     F.col("c.origin_country").cast("string").alias("company_country"),
                     F.col("_ingested_ts").alias("_ingested_at"), "_batch_id"))

cast_t = (base.select(F.col("id").cast("long").alias("movie_id"),
                      F.explode("credits.cast").alias("p"),
                      "_ingested_ts", "_batch_id")
          .select("movie_id",
                  F.col("p.credit_id").cast("string").alias("credit_id"),
                  F.col("p.id").cast("long").alias("person_id"),
                  F.col("p.name").cast("string").alias("person_name"),
                  F.col("p.character").cast("string").alias("character_name"),
                  F.col("p.order").cast("int").alias("cast_order"),
                  F.col("p.gender").cast("int").alias("gender"),
                  F.col("p.known_for_department").cast("string").alias("known_for_department"),
                  F.col("p.popularity").cast("double").alias("person_popularity"),
                  F.col("_ingested_ts").alias("_ingested_at"), "_batch_id"))

crew_t = (base.select(F.col("id").cast("long").alias("movie_id"),
                      F.explode("credits.crew").alias("p"),
                      "_ingested_ts", "_batch_id")
          .select("movie_id",
                  F.col("p.credit_id").cast("string").alias("credit_id"),
                  F.col("p.id").cast("long").alias("person_id"),
                  F.col("p.name").cast("string").alias("person_name"),
                  F.col("p.job").cast("string").alias("job"),
                  F.col("p.department").cast("string").alias("department"),
                  F.col("p.gender").cast("int").alias("gender"),
                  F.col("p.known_for_department").cast("string").alias("known_for_department"),
                  F.col("p.popularity").cast("double").alias("person_popularity"),
                  F.col("_ingested_ts").alias("_ingested_at"), "_batch_id"))

upsert(genres,    "silver.movie_genres",    ["movie_id", "genre_id"],  delete_missing=True)
upsert(companies, "silver.movie_companies", ["movie_id", "company_id"], delete_missing=True)
upsert(cast_t,    "silver.movie_cast",      ["movie_id", "credit_id"],  delete_missing=True)
upsert(crew_t,    "silver.movie_crew",      ["movie_id", "credit_id"],  delete_missing=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- acceptance checks --------------------------------------------------------------------

m  = spark.table("silver.movies")
bd = spark.table("bronze.movie_details_raw")

parent_n   = m.count()
bronze_ids = bd.select("id").distinct().count()
dupes      = m.groupBy("movie_id").count().filter("count > 1").count()

print("=" * 58)
print(f"silver.movies rows        : {parent_n}")
print(f"bronze distinct ids       : {bronze_ids}   match={parent_n == bronze_ids}")
print(f"duplicate movie_id        : {dupes}   (must be 0)")
print(f"null movie_id             : {m.filter(F.col('movie_id').isNull()).count()}   (must be 0)")
print(f"null _ingested_at         : {m.filter(F.col('_ingested_at').isNull()).count()}   (must be 0)")
print(f"null release_date         : {m.filter(F.col('release_date').isNull()).count()}")
print("-" * 58)

for tbl, keycol in [("silver.movie_genres", "genre_id"),
                    ("silver.movie_companies", "company_id"),
                    ("silver.movie_cast", "credit_id"),
                    ("silver.movie_crew", "credit_id")]:
    c = spark.table(tbl)
    n = c.count()
    orphans = c.join(m, "movie_id", "left_anti").count()
    print(f"{tbl:<26} {n:>8} rows   orphans={orphans}   >=parent={n >= parent_n}")

print("=" * 58)
print("Now RUN THIS NOTEBOOK AGAIN — every number above must be identical.")
print("That is the real test of MERGE: idempotence, not row counts.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
