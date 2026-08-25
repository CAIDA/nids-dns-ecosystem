[README](README.md) | [Introduction](Introduction.md) | [Datasets](Datasets.md) | [Spark](Spark.md) | [Tasks](Tasks.md) | Task 1 ⮕ | [Task 2](Task-2-resilience.md) | [Task 3](Task-3-security.md) | [Notebook](nids-dns-ecosystem.ipynb)

# Task 1 Guidance: DNS Hosting Centralization

This page provides implementation guidance for Task 1 of [nids-dns-ecosystem.ipynb](nids-dns-ecosystem.ipynb).

---

## Extracting the DNS Provider from an NS Hostname

An NS record stores the **hostname** of a nameserver, not the IP address. For example:

```
example.se.  IN NS  ns1.cloudflare.com.
example.se.  IN NS  ns2.cloudflare.com.
anotherdomain.se.  IN NS  ns-123.awsdns-45.com.
anotherdomain.se.  IN NS  ns-456.awsdns-12.org.
```

To identify the DNS hosting provider, extract the **registrable domain** from each NS hostname — the domain immediately below its public suffix:

| NS hostname | Registrable domain (provider) |
|---|---|
| `ns1.cloudflare.com` | `cloudflare.com` |
| `ns-123.awsdns-45.com` | `amazon.com`? No — `awsdns-45.com`! |
| `pdns1.registrar-servers.com` | `registrar-servers.com` |
| `dns1.p09.nsone.net` | `nsone.net` |

Use `tldextract` (not a simple string split) because public suffixes are not always a single label — `co.uk`, `com.br`, and `pvt.k12.ma.us` are all public suffixes, and a naive split on `.` would give wrong results.

```python
import tldextract

def extract_provider(ns_hostname):
    ext = tldextract.extract(str(ns_hostname))
    if ext.domain and ext.suffix:
        return f"{ext.domain}.{ext.suffix}"
    return str(ns_hostname)

# Example
extract_provider("ns1.cloudflare.com")       # → "cloudflare.com"
extract_provider("ns-123.awsdns-45.com")     # → "awsdns-45.com"
extract_provider("pdns1.registrar-servers.com")  # → "registrar-servers.com"
```

> **Note on AWS**: Amazon Route 53 nameservers use hostnames like `ns-123.awsdns-45.com`, `ns-456.awsdns-12.net`, `ns-789.awsdns-34.org`, `ns-012.awsdns-56.co.uk`. Their registrable domains differ! You may want to post-process these so all four collapse to a single provider label. One approach: if the hostname ends with `awsdns-*.com`, `awsdns-*.net`, `awsdns-*.org`, or `awsdns-*.co.uk`, return `amazonaws.com`. Whichever label you pick, use it consistently — otherwise Route 53's market share is split four ways and both the top-N ranking and the HHI come out too low.

---

## Counting Domains per Provider

A domain may list NS records from **multiple providers** (e.g., two independent providers for extra resilience). Count each provider at most once per domain using Spark's `collect_set` aggregation:

```python
import pyspark.sql.functions as psf

# Add provider column
ns_with_provider = ns_df.withColumn("provider", extract_provider_udf(psf.col("ns_address")))

# Per domain: collect the set of distinct providers
domain_providers = (
    ns_with_provider
    .groupBy("query_name")
    .agg(psf.collect_set("provider").alias("providers"))
)

# Explode to (query_name, provider) pairs, then count distinct domains per provider
from pyspark.sql.functions import explode

provider_counts = (
    domain_providers
    .select("query_name", explode("providers").alias("provider"))
    .groupBy("provider")
    .agg(psf.countDistinct("query_name").alias("domain_count"))
    .orderBy(psf.col("domain_count").desc())
)
provider_counts.show(10)
```

---

## Market Share and HHI

Collect the small aggregated result to the driver, then compute HHI in Python:

```python
# Collect to driver — only one row per provider (hundreds, not millions)
counts_pd = provider_counts.toPandas()
total_domains = ns_df.select("query_name").distinct().count()

market_share = {
    row["provider"]: row["domain_count"] / total_domains
    for _, row in counts_pd.iterrows()
}

# HHI = sum of squared market shares
hhi = sum(s**2 for s in market_share.values())
```

Interpreting HHI:
- Near **0**: many providers of roughly equal size (competitive market)
- **0.15–0.25**: moderately concentrated
- Above **0.25**: highly concentrated (antitrust concern threshold)
- **1.0**: pure monopoly

---

## Plotting the CCDF

Collect the aggregated `provider_counts` DataFrame (one row per provider) to the driver for plotting:

```python
import numpy as np
import matplotlib.pyplot as plt

counts_pd = provider_counts.toPandas()  # small — one row per provider
x, y = ccdf(counts_pd["domain_count"])

fig, ax = plt.subplots(figsize=(8, 5))
ax.loglog(x, y, marker='.', linewidth=0, markersize=3)
ax.set_xlabel("Domains served per provider")
ax.set_ylabel("CCDF (fraction of providers with ≥ x domains)")
ax.grid(True, which="both", linestyle="--", alpha=0.4)
plt.tight_layout()
```

---

## Longitudinal Analysis Tips

Load all years at once with `load_longitudinal()`. The `year` partition column is retained automatically, so a single `groupBy("year")` aggregation replaces a manual loop:

```python
YEARS   = [2019, 2020, 2021, 2022, 2023, 2024]
df_long = load_longitudinal(SOURCE, YEARS)

ns_long = (
    df_long.filter(psf.col("response_type") == "NS")
           .select("year", "query_name", "ns_address")
           .dropna()
           .withColumn("provider", extract_provider_udf(psf.col("ns_address")))
)

# Total domains per year — distinct NS query_names, the same denominator the
# snapshot code above uses. (Do not substitute the SOA population here; see the
# denominator note in Datasets.md.)
total_per_year = (
    ns_long.select("year", "query_name").distinct()
           .groupBy("year").agg(psf.countDistinct("query_name").alias("total"))
           .toPandas().set_index("year")["total"].to_dict()
)

# Per-year provider counts (collected to driver). No collect_set/explode round
# trip is needed here: countDistinct("query_name") already counts each domain
# once per provider, which is exactly what the explode was there to guarantee.
year_provider_counts = (
    ns_long.groupBy("year", "provider")
           .agg(psf.countDistinct("query_name").alias("domain_count"))
           .toPandas()
)

# Compute HHI per year in Python
year_stats = {}
for year, grp in year_provider_counts.groupby("year"):
    shares = grp["domain_count"] / total_per_year[year]
    hhi_y  = (shares ** 2).sum()
    top3   = shares.nlargest(3).sum()
    year_stats[year] = {"hhi": hhi_y, "top3_share": top3}
```

[README](README.md) | [Introduction](Introduction.md) | [Datasets](Datasets.md) | [Spark](Spark.md) | [Tasks](Tasks.md) | Task 1 ⮕ | [Task 2](Task-2-resilience.md) | [Task 3](Task-3-security.md) | [Notebook](nids-dns-ecosystem.ipynb)
