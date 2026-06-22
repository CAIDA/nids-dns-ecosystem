[README](README.md) | [Introduction](Introduction.md) | Datasets ⮕ | [Tasks](Tasks.md) | [Task 1](Task-1-centralization.md) | [Task 2](Task-2-resilience.md) | [Task 3](Task-3-security.md) | [Notebook](nids-dns-ecosystem.ipynb)

# Datasets

---

## OpenINTEL Forward DNS Measurements

[OpenINTEL](https://openintel.nl) performs daily active DNS measurements by querying every registered domain name in a TLD's zone file. For each domain it sends a fixed set of DNS query types (A, AAAA, NS, MX, TXT, SOA, DS, DNSKEY, CAA, and more) and stores all responses. The result is a daily snapshot of the DNS state across hundreds of millions of domains, stretching back to 2015 for `.com` and to 2016 for `.nl` and `.se`.

### Available TLDs

OpenINTEL measures many TLDs. A subset are **open access** — no registration required:

| TLD | Since | Notes |
|-----|-------|-------|
| `.gov` | 2017-05-01 | US Federal government domains only (~6 k domains) |
| `.se` | 2016-06-07 | Sweden (~1 M domains) — good general-purpose TLD |
| `.nu` | 2016-06-07 | Niue / Swedish speakers |
| `.ee` | 2019-07-29 | Estonia |
| `.ch` | 2020-05-19 | Switzerland |
| `.li` | 2020-05-19 | Liechtenstein |
| `.sk` | 2022-05-11 | Slovakia |
| `.fr` | 2022-08-10 | France (~4 M domains) |

For this assignment, **`.se`** is recommended for its size (large enough to be statistically meaningful, small enough to process on a laptop), historical coverage (2016–present), and general-purpose domain mix. Use **`.gov`** if you want a smaller dataset to prototype your code first.

### Data Format

OpenINTEL data is stored as **Apache Parquet** files, partitioned by `source` (TLD), `year`, `month`, and `day`. Each file covers one TLD for one day.

The schema uses **sparse storage**: every row has the same columns, but only the fields relevant to that row's record type are non-null. For example, an NS record row will have `ns_address` filled in but `ds_algorithm` will be null.

Key fields used in this assignment:

| Field | Type | Description |
|-------|------|-------------|
| `query_name` | STRING | The domain that was queried (e.g., `example.se`) |
| `query_type` | STRING | The query type sent (`A`, `NS`, `TXT`, …) |
| `response_type` | STRING | The record type in the response (may differ from query type, e.g. CNAME) |
| `response_ttl` | INTEGER | Time-to-live of the returned record |
| `ns_address` | STRING | NS hostname from an NS record (e.g., `ns1.cloudflare.com`) |
| `txt_text` | STRING | Full text of a TXT record (concatenated) |
| `ds_algorithm` | INTEGER | Algorithm ID from a DS record |
| `ds_digest_type` | INTEGER | Hash algorithm ID from a DS record |
| `dnskey_algorithm` | INTEGER | Algorithm ID from a DNSKEY record |
| `dnskey_flags` | INTEGER | Flags field: 256 = ZSK, 257 = KSK |
| `caa_flags` | INTEGER | CAA flags field |
| `caa_tag` | STRING | CAA tag: `issue`, `issuewild`, `iodef` |
| `caa_value` | STRING | CAA value (CA domain or `;`) |
| `soa_mname` | STRING | Primary master name server from SOA |
| `soa_refresh` | LONG | SOA refresh interval in seconds |
| `soa_retry` | LONG | SOA retry interval in seconds |
| `soa_expire` | LONG | SOA expire interval in seconds |
| `soa_minimum` | LONG | SOA minimum TTL / negative caching TTL |
| `ad_flag` | INTEGER | 1 if the response was DNSSEC-validated by the worker's resolver |

> **Note**: NS-to-IP and MX-to-IP resolution is performed in a separate OpenINTEL infrastructure measurement dataset that is not publicly available. The zone-based data only contains NS *hostnames*, not their resolved IP addresses. In Task 2, you will resolve these hostnames yourself using `dnspython`.

### Accessing the Data via Spark

OpenINTEL data is stored in a public S3-compatible object store and is accessed directly with **PySpark + S3A** — no credentials or manual download required. PySpark reads the partitioned Parquet files in parallel directly from the object store, which makes both snapshot and multi-year longitudinal analysis efficient.

| Setting | Value |
|---------|-------|
| Endpoint | `https://object.openintel.nl` |
| Bucket | `openintel-public` |
| Partition path | `fdns/basis=zonefile/source={source}/year={year}/month={month:02d}/day={day:02d}/` |
| File format | `gz.parquet` (gzip-compressed Parquet, multiple part files per partition) |

The partition columns `source`, `year`, `month`, and `day` are not stored inside the files — Spark infers them from the path when you pass a `basePath` option. Use them in `.filter()` calls for partition pruning.

#### SparkSession setup

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
conf.set("spark.executor.memory", "4G")
conf.set("spark.driver.memory", "4G")
conf.set("fs.s3a.impl",                       "org.apache.hadoop.fs.s3a.S3AFileSystem")
conf.set("fs.s3a.aws.credentials.provider",   "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
conf.set("fs.s3a.endpoint",                   OI_ENDPOINT)
conf.set("fs.s3a.connection.ssl.enabled",     "true")
conf.set("fs.s3a.path.style.access",          "true")
conf.set("fs.s3a.block.size",                 "64M")
conf.set("fs.s3a.readahead.range",            "4M")
conf.set("spark.sql.parquet.mergeSchema",     "false")
conf.set("spark.sql.parquet.filterPushdown",  "true")
conf.set(
    "spark.jars.packages",
    "org.apache.hadoop:hadoop-aws:3.4.0,"
    "software.amazon.awssdk:bundle:2.24.6"
)
spark = SparkSession.builder.config(conf=conf).getOrCreate()
```

#### Loading a single snapshot

```python
SOURCE = "gov"   # start with .gov (~15 MB/day) to prototype; switch to .se (~1 GB/day) for full analysis

base = f"s3a://{OI_BUCKET}/{OI_FDNS_BASE}"
snap_path = f"{base}/source={SOURCE}/year=2024/month=01/day=15"

df_snap = spark.read.option("basePath", base + "/").parquet(snap_path)
df_snap.printSchema()
```

#### Loading multiple years (longitudinal)

Because the `year` column is a Hive-style partition, loading several years at once is one `spark.read` call. Spark performs **partition pruning** automatically.

```python
YEARS   = [2019, 2020, 2021, 2022, 2023, 2024]
long_paths = [f"{base}/source={SOURCE}/year={y}/month=01/day=15" for y in YEARS]

df_long = spark.read.option("basePath", base + "/").parquet(*long_paths)
# df_long.columns includes 'year', 'month', 'day', 'source' from the partition path
```

#### Filtering and aggregating

```python
# Count registered domains per year (SOA appears exactly once per registered domain)
df_long.filter(psf.col("response_type") == "SOA") \
       .groupBy("year") \
       .agg(psf.count("query_name").alias("domain_count")) \
       .orderBy("year").show()
```

#### Collecting to pandas for plotting

All Spark aggregations produce small result DataFrames that are collected to the driver with `.toPandas()` for visualization with `matplotlib`. **Never call `.toPandas()` on a large unfiltered DataFrame** — only on the final aggregated result.

```python
result_pd = df_long.filter(...).groupBy(...).agg(...).orderBy("year").toPandas()
```

> **TLD size guidance**: `.gov` (~25 k domains/day) and `.li` (~85 k) are ideal for development. `.se` (~1 M) and `.fr` (~4 M) are larger — these are better suited to running on a cluster (e.g., NRP's JupyterHub) rather than a laptop.

---

## LACeS Anycast Census

The **LACeS Anycast Census** is a daily census of anycast-announced IPv4 (and IPv6) prefixes, produced by the University of Twente. It uses two detection methods:

- **AB (Anycast-Based)**: sends ICMP probes from many vantage points; a high response count indicates multiple instances, suggesting anycast.
- **GCD (Latency-Based)**: analyses RTT distributions from distributed vantage points to detect routing to different physical locations.

The census reports results at **/24 prefix granularity**. Each row represents a /24 prefix with its AB and GCD scores.

### Confidence Filtering

| Filter | Use case |
|--------|----------|
| `(AB > 3) \| (GCD > 1)` | **High confidence** — recommended for research |
| `(AB > 1) \| (GCD > 1)` | **Comprehensive** — maximises coverage, includes borderline cases |

### Loading the Census with Spark

The census is a small file (~a few MB). Download it once to a local path, then load it into Spark.

```python
import requests

ANYCAST_URL   = "https://manycast.net/api/v1/export/IPv4-latest.parquet"
ANYCAST_LOCAL = "/tmp/anycast-census.parquet"

resp = requests.get(ANYCAST_URL, timeout=60)
resp.raise_for_status()
with open(ANYCAST_LOCAL, "wb") as f:
    f.write(resp.content)

census_df = spark.read.parquet(ANYCAST_LOCAL)
census_df.printSchema()

# Collect high-confidence anycast /24s to the driver as a Python set
# (small enough to fit in memory; used for lookups in Task 2)
anycast_set = set(
    census_df.filter((psf.col("AB") > 3) | (psf.col("GCD") > 1))
             .select("prefix")
             .rdd.flatMap(lambda r: [r[0]])
             .collect()
)
print(f"Anycast /24 prefixes (high confidence): {len(anycast_set)}")
```

Full API documentation: [https://manycast.net/api/docs](https://manycast.net/api/docs)

### Census Schema

| Field | Description |
|-------|-------------|
| `prefix` | /24 prefix string (e.g., `1.2.3.0/24`) |
| `AB` | Anycast-Based score (number of distinct vantage points responding) |
| `GCD` | 1 if latency-based method detected anycast, 0 otherwise |
| `asn` | ASN of the prefix |
| `as_name` | Human-readable AS name |

---

## dnspython (NS Hostname Resolution)

Because the OpenINTEL zone-based data does not include resolved NS IP addresses, Task 2 resolves NS hostnames to IPs on the driver using **dnspython**. Since the DNS hosting market is concentrated, resolving only the top-N most common NS hostnames covers the vast majority of domains.

```python
import dns.resolver

def resolve_hostname(hostname, rdtype="A", timeout=5):
    """Resolve hostname → list of IP strings on the driver. Returns [] on failure."""
    resolver = dns.resolver.Resolver()
    resolver.timeout = timeout
    resolver.lifetime = timeout
    try:
        return [str(rr) for rr in resolver.resolve(hostname, rdtype)]
    except Exception:
        return []
```

The resolved hostname→IP mapping is then broadcast to all Spark workers as a `pyspark.broadcast` variable so it can be used efficiently in Spark UDFs without re-resolving on every row.

[README](README.md) | [Introduction](Introduction.md) | Datasets ⮕ | [Tasks](Tasks.md) | [Task 1](Task-1-centralization.md) | [Task 2](Task-2-resilience.md) | [Task 3](Task-3-security.md) | [Notebook](nids-dns-ecosystem.ipynb)
