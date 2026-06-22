[README](README.md) | [Introduction](Introduction.md) | [Datasets](Datasets.md) | [Tasks](Tasks.md) | [Task 1](Task-1-centralization.md) | Task 2 ⮕ | [Task 3](Task-3-security.md) | [Notebook](nids-dns-ecosystem.ipynb)

# Task 2 Guidance: DNS Infrastructure Resilience & Anycast

This page provides implementation guidance for Task 2 of [nids-dns-ecosystem.ipynb](nids-dns-ecosystem.ipynb).

---

## NS Server Count per Domain

```python
import pyspark.sql.functions as psf

ns_count_df = (
    ns_df.groupBy("query_name")
         .agg(psf.countDistinct("ns_address").alias("ns_count"))
)

# Distribution: bucket into 1, 2, 3, 4, >=5
from pyspark.sql.functions import when

dist_df = (
    ns_count_df
    .withColumn("bucket",
        when(psf.col("ns_count") >= 5, ">=5")
        .otherwise(psf.col("ns_count").cast("string"))
    )
    .groupBy("bucket").count()
    .orderBy("bucket")
)
total = ns_count_df.count()
dist_df.withColumn("fraction", psf.col("count") / total).show()
```

RFC 1034 §4.1 states: *"By administrative fiat we require every zone to be available on at least two servers."* Domains with fewer than two distinct NS entries violate this requirement.

---

## Provider Diversity

Reuse `domain_providers` (query_name, providers array) from Task 1. Use `psf.size()` to count providers per domain:

```python
from pyspark.sql.functions import size

provider_diversity_df = domain_providers.withColumn(
    "provider_count", size(psf.col("providers"))
)

total = provider_diversity_df.count()
single = provider_diversity_df.filter(psf.col("provider_count") == 1).count()
print(f"Single-provider domains: {single / total:.1%}")
```

Note the distinction: a domain can have **multiple NS hostnames all from the same provider** (common with providers that offer ns1/ns2/ns3/ns4 pairs). That domain has high NS count but low provider diversity — it remains dependent on one organisation.

---

## SOA Parameter RFC Guidelines

```python
soa_df = df_snap.filter(psf.col("response_type") == "SOA") \
                .select("query_name", "soa_refresh", "soa_retry",
                        "soa_expire", "soa_minimum") \
                .dropna(subset=["query_name"])

soa_df.describe(["soa_refresh", "soa_retry", "soa_expire", "soa_minimum"]).show()

total_soa = soa_df.count()
low_expire  = soa_df.filter(psf.col("soa_expire")  < 604800).count()
high_min    = soa_df.filter(psf.col("soa_minimum") > 86400).count()

print(f"Low expire  (< 1 week): {low_expire  / total_soa:.1%}")
print(f"High minimum (> 1 day): {high_min    / total_soa:.1%}")
```

---

## Resolving NS Hostnames with dnspython

Because OpenINTEL zone-based data does not include NS IP addresses, you resolve them with the system resolver using `dnspython`. Resolving all unique NS hostnames can be slow; instead resolve only the most frequently occurring ones (which cover most domains, given the concentrated market):

```python
import dns.resolver, dns.exception
from collections import Counter

def resolve_hostname(hostname, rdtype="A", timeout=5):
    r = dns.resolver.Resolver()
    r.timeout = timeout
    r.lifetime = timeout
    try:
        return [str(rr) for rr in r.resolve(hostname, rdtype)]
    except Exception:
        return []

# Take the top-500 NS hostnames by frequency — these cover most domains
top_ns_hostnames = [hn for hn, _ in Counter(ns_raw['ns_address']).most_common(500)]

ns_ips = {}  # hostname → list of IPv4 strings
for hostname in top_ns_hostnames:
    ips = resolve_hostname(hostname, "A")
    if ips:
        ns_ips[hostname] = ips
```

> **Why top-500?** In practice, the top few hundred NS hostnames cover the majority of domains because the DNS hosting market is concentrated. Resolving 500 hostnames takes 1–2 minutes; resolving all unique hostnames could take hours.

---

## Anycast Cross-Reference

The NS resolution step runs on the **driver** (dnspython is not Spark-aware). Broadcast the results so UDFs can use them without re-resolving on every row:

```python
# Broadcast the hostname→IPs and IP→anycast maps to all workers
bc_ns_ips     = spark.sparkContext.broadcast(ns_ips)     # {hostname: [ip, ...]}
bc_ip_anycast = spark.sparkContext.broadcast(ns_ip_anycast)  # {ip: bool}

@psf.udf("boolean")
def ns_is_anycast_udf(hostname):
    """Return True if any resolved IP for this NS hostname is anycast."""
    if hostname is None:
        return False
    for ip in bc_ns_ips.value.get(hostname, []):
        if bc_ip_anycast.value.get(ip, False):
            return True
    return False

# Domain-level: at least one anycast NS
anycast_flag_df = (
    ns_df.withColumn("anycast", ns_is_anycast_udf(psf.col("ns_address")))
         .groupBy("query_name")
         .agg(psf.max("anycast").alias("has_anycast"))
)
n_anycast = anycast_flag_df.filter(psf.col("has_anycast")).count()
total     = anycast_flag_df.count()
print(f"Domains with ≥1 anycast NS: {n_anycast / total:.1%}")
```

### Anycast by provider

```python
from pyspark.sql.functions import explode

provider_anycast = (
    ns_df.withColumn("provider", extract_provider_udf(psf.col("ns_address")))
         .withColumn("anycast", ns_is_anycast_udf(psf.col("ns_address")))
         .groupBy("provider")
         .agg(
             psf.count("ns_address").alias("total_ns_rows"),
             psf.sum(psf.col("anycast").cast("int")).alias("anycast_ns_rows"),
         )
         .withColumn("anycast_frac", psf.col("anycast_ns_rows") / psf.col("total_ns_rows"))
         .orderBy(psf.col("total_ns_rows").desc())
)
provider_anycast.show(20)
```

[README](README.md) | [Introduction](Introduction.md) | [Datasets](Datasets.md) | [Tasks](Tasks.md) | [Task 1](Task-1-centralization.md) | Task 2 ⮕ | [Task 3](Task-3-security.md) | [Notebook](nids-dns-ecosystem.ipynb)
