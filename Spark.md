[README](README.md) | [Introduction](Introduction.md) | [Datasets](Datasets.md) | Spark ⮕ | [Tasks](Tasks.md) | [Task 1](Task-1-centralization.md) | [Task 2](Task-2-resilience.md) | [Task 3](Task-3-security.md) | [Notebook](nids-dns-ecosystem.ipynb)

# Spark Guide: Querying OpenINTEL with PySpark

This guide builds up the PySpark you need for the three tasks, one piece at a
time. Every example reads from the `fdns/basis=zonefile` OpenINTEL layout
described in [Datasets](Datasets.md); read that page first for the field list
and TLD sizes, then come back here for how to actually load, filter, and
aggregate them at scale.

Each [Task guide](Tasks.md) contains the complete, commented query for that
task. This page explains the *building blocks* those queries are assembled
from.

## 1. Starting a SparkSession

OpenINTEL's Parquet files live in a public S3-compatible object store, so the
session needs the S3A connector configured for anonymous access. This is
boilerplate — copy it once per notebook:

```python
import time
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as psf

OI_ENDPOINT  = "https://object.openintel.nl"
OI_BUCKET    = "openintel-public"
OI_FDNS_BASE = "fdns/basis=zonefile"

conf = SparkConf()
conf.setMaster("local[*]")
conf.setAppName(f"dns-ecosystem-{int(time.time())}")
conf.set("spark.executor.memory",                      "8G")
conf.set("spark.driver.memory",                        "8G")
conf.set("fs.s3a.impl",                                "org.apache.hadoop.fs.s3a.S3AFileSystem")
conf.set("fs.s3a.aws.credentials.provider",            "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
conf.set("fs.s3a.endpoint",                            OI_ENDPOINT)
conf.set("fs.s3a.connection.ssl.enabled",              "true")
conf.set("fs.s3a.path.style.access",                   "true")
conf.set("fs.s3a.block.size",                          "64M")
conf.set("fs.s3a.readahead.range",                     "4M")
conf.set("fs.s3a.vectored.io.enabled",                 "false")
conf.set("parquet.hadoop.vectored.io.enabled",         "false")
conf.set("spark.sql.parquet.mergeSchema",              "false")
conf.set("spark.sql.parquet.filterPushdown",           "true")
conf.set(
    "spark.jars.packages",
    "org.apache.hadoop:hadoop-aws:3.4.0,"
    "software.amazon.awssdk:bundle:2.24.6"
)
spark = SparkSession.builder.config(conf=conf).getOrCreate()
```

The `fs.s3a.vectored.io.enabled` / `parquet.hadoop.vectored.io.enabled` pair
disables Hadoop's vectored-read path — leave both `"false"`; enabling them
against this particular object store produces read failures, not a speed-up.

Call `spark.stop()` in a final cell once you're done, so the JVM and any
executor threads shut down cleanly.

## 2. Loading data

### 2.1 Single snapshot

`spark.read.parquet(...)` accepts one or many paths. Passing `basePath` tells
Spark where the Hive-style partitioning (`source=`, `year=`, `month=`,
`day=`) starts, so it can parse those segments back into real columns instead
of treating them as opaque path text:

```python
def load_snapshot(source, year, month=1, day=15):
    """Load a single-day OpenINTEL snapshot as a Spark DataFrame."""
    base = f"s3a://{OI_BUCKET}/{OI_FDNS_BASE}"
    path = f"{base}/source={source}/year={year}/month={month:02d}/day={day:02d}"
    return spark.read.option("basePath", base + "/").parquet(path)
```

### 2.2 Longitudinal (multiple years)

Pass a *list* of paths to `spark.read.parquet(...)` and Spark unions them into
one DataFrame in a single call — the `year` partition column is retained, so a
later `groupBy("year")` computes every year's aggregate in one pass instead of
looping in Python:

```python
def load_longitudinal(source, years, month=1, day=15):
    """Load one snapshot per year as a single Spark DataFrame (year column retained)."""
    base  = f"s3a://{OI_BUCKET}/{OI_FDNS_BASE}"
    paths = [f"{base}/source={source}/year={y}/month={month:02d}/day={day:02d}" for y in years]
    return spark.read.option("basePath", base + "/").parquet(*paths)
```

```python
YEARS   = [2019, 2020, 2021, 2022, 2023, 2024]
df_long = load_longitudinal(SOURCE, YEARS)
```

## 3. Filtering and selecting

The sparse schema means most work starts by filtering to one `response_type`
and selecting just the fields that record type populates:

```python
ns_df = df_snap.filter(psf.col("response_type") == "NS") \
               .select("query_name", "ns_address") \
               .dropna()
```

- **Filter before you select.** `filter()` first narrows the row set;
  `select()` then narrows the column set — order doesn't change correctness
  here, but reads more naturally as "which rows, then which fields."
- **`.dropna()`** drops rows where the selected columns are null — useful
  right after narrowing to a `response_type`, since the sparse schema
  guarantees the *other* record types' fields are null, but occasionally a
  field within the type you asked for is null too (a malformed response).
- **Select only the columns you need.** Every unselected column is sparse
  storage you don't pay to read — pulling all of them out of habit costs real
  I/O across millions of rows for no benefit.

## 4. UDFs vs. built-ins

Reach for a built-in `pyspark.sql.functions` (`psf`) method first — it runs in
the JVM without round-tripping each row through Python. Write a `@psf.udf`
only when the transformation needs a Python library Spark doesn't have a
built-in for (`tldextract`, a regex with backreferences, etc.):

```python
from pyspark.sql.types import StringType
import tldextract

@psf.udf(StringType())
def extract_provider_udf(ns_hostname):
    """Map an NS hostname to its registrable domain (DNS hosting provider)."""
    if ns_hostname is None:
        return None
    ext = tldextract.extract(ns_hostname)
    if ext.domain and ext.suffix:
        return f"{ext.domain}.{ext.suffix}"
    return ns_hostname
```

```python
ns_with_provider = ns_df.withColumn(
    "provider", extract_provider_udf(psf.col("ns_address"))
)
```

The same pattern extracts the SPF `all`-qualifier with a regex UDF
(`spf_qualifier_udf`) — declare the return type explicitly (`StringType()`),
and return `None` for missing input rather than raising, since a UDF
exception aborts the whole task.

## 5. Collapsing the grain before you fan out

A domain can have several NS records pointing at providers that repeat (two
nameservers from the same company). Counting providers *per domain* first,
then exploding back out to count domains *per provider*, avoids
double-counting a provider that appears on multiple NS rows for one domain —
the same "collapse the grain before the fan-out" idea SQL uses a CTE for:

```python
# 1. One row per (domain, distinct provider SET) — collapses duplicate providers per domain
domain_providers = (
    ns_with_provider
    .groupBy("query_name")
    .agg(psf.collect_set("provider").alias("providers"))
)

# 2. Explode back to (domain, provider) pairs, now de-duplicated, then count distinct domains per provider
provider_counts = (
    domain_providers
    .select("query_name", psf.explode("providers").alias("provider"))
    .groupBy("provider")
    .agg(psf.countDistinct("query_name").alias("domain_count"))
    .orderBy(psf.col("domain_count").desc())
)
```

`collect_set` (not `collect_list`) is what makes this a de-duplicating step —
a `list` would keep repeats and inflate the later explode.

## 6. Joins: inner vs. anti

Use an inner join to keep only rows present on both sides (e.g., domains with
*both* a DS and a DNSKEY record — properly secured):

```python
n_secured = (
    ds_df.select("query_name").distinct()
         .join(dnskey_df.select("query_name").distinct(), "query_name", "inner")
         .count()
)
```

Use `"left_anti"` to keep only rows on the left with **no** match on the
right — this is how you find "orphaned" DNSKEYs (a domain publishes a key but
never got a DS record at the parent, so the chain of trust is broken):

```python
n_orphan = (
    dnskey_df.select("query_name").distinct()
             .join(ds_df.select("query_name").distinct(), "query_name", "left_anti")
             .count()
)
```

Reduce each side to `distinct()` `query_name`s before joining — you're testing
set membership, not carrying row-level fields across, so there's no reason to
join the full (possibly repeated) record sets.

## 7. Broadcast variables

Task 2's anycast cross-reference needs a Python `dict` (hostname → resolved
IPs, IP → anycast bool) available inside a UDF that runs on every executor.
Shipping a plain closure works but re-serializes the dict with every task;
`spark.sparkContext.broadcast(...)` sends it once per executor and caches it:

```python
bc_ns_ips     = spark.sparkContext.broadcast(ns_ips)
bc_ip_anycast = spark.sparkContext.broadcast(ns_ip_anycast)

@psf.udf("boolean")
def ns_is_anycast_udf(hostname):
    if hostname is None:
        return False
    for ip in bc_ns_ips.value.get(hostname, []):
        if bc_ip_anycast.value.get(ip, False):
            return True
    return False
```

Access the underlying value with `.value` inside the UDF — never reference
the plain Python `dict` directly once a broadcast wrapper exists, or you lose
the caching benefit.

## 8. Collecting to pandas

Spark aggregations produce small result DataFrames that are collected to the
driver with `.toPandas()` for plotting or row-wise Python logic (e.g. the
`ccdf()` helper below, or `.apply()` with a haversine-style distance
function). **Never call `.toPandas()` on a large, unfiltered DataFrame** —
only on the final, already-aggregated result:

```python
counts_pd = provider_counts.toPandas()   # one row per provider — small
```

Once collected, it's a normal pandas DataFrame:

```python
import numpy as np

def ccdf(values):
    """Return (x, y) arrays for a CCDF — sorted descending, y = fraction >= x."""
    arr = np.sort(np.array(values, dtype=float))[::-1]
    y   = np.arange(1, len(arr) + 1) / len(arr)
    return arr, y
```

For longitudinal results, collect the per-year aggregate (small — one row per
year, or per year × provider) and finish the per-year math (shares, HHI) in
pandas rather than in Spark:

```python
provider_pd = provider_per_year.toPandas()   # (year, provider, domain_count) — still small

year_stats = {}
for year, grp in provider_pd.groupby("year"):
    shares = grp["domain_count"] / grp["domain_count"].sum()
    year_stats[year] = {"hhi": (shares ** 2).sum()}
```

## 9. Writing efficient Spark jobs

- **Filter on `response_type` and partition columns first.** A `.filter()` on
  `source`/`year`/`month`/`day` prunes whole files before Spark ever reads
  them; a `.filter()` on `response_type` still avoids carrying irrelevant
  sparse columns into later steps.
- **Select only the columns you need**, right after filtering — see §3.
- **Prefer built-in `psf` functions over UDFs** — see §4 — and give every UDF
  an explicit return type.
- **Collapse the grain before you fan out** (`collect_set` → `explode`, not
  a raw self-join) — see §5.
- **Join distinct key sets, not raw repeated rows**, when you only care about
  membership — see §6.
- **Broadcast, don't re-ship, driver-side lookup dictionaries** used inside a
  UDF — see §7.
- **Iterate on `.gov`, submit on `.se`.** `.gov` is ~15 MB/day and iterates in
  seconds, so debug your query shape there; then set `SOURCE = "se"` and re-run
  every cell — `.se` is the graded dataset (see [Datasets](Datasets.md)).
- **Never `.toPandas()` an unfiltered DataFrame** — see §8.

[README](README.md) | [Introduction](Introduction.md) | [Datasets](Datasets.md) | Spark ⮕ | [Tasks](Tasks.md) | [Task 1](Task-1-centralization.md) | [Task 2](Task-2-resilience.md) | [Task 3](Task-3-security.md) | [Notebook](nids-dns-ecosystem.ipynb)
