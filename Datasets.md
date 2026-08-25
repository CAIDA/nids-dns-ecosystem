[README](README.md) | [Introduction](Introduction.md) | Datasets ⮕ | [Spark](Spark.md) | [Tasks](Tasks.md) | [Task 1](Task-1-centralization.md) | [Task 2](Task-2-resilience.md) | [Task 3](Task-3-security.md) | [Notebook](nids-dns-ecosystem.ipynb)

# Datasets

---

## OpenINTEL Forward DNS Measurements

[OpenINTEL](https://openintel.nl) performs daily active DNS measurements by querying every registered domain name in a TLD's zone file. For each domain it sends a fixed set of DNS query types (A, AAAA, NS, MX, TXT, SOA, DS, DNSKEY, CAA, and more) and stores all responses. The result is a daily snapshot of the DNS state across hundreds of millions of domains, stretching back to 2015 for `.com` and to 2016 for `.nl` and `.se`. Note that `.com` and `.nl` are **closed** datasets — they are not in the public object store, so only the open-access TLDs listed below can be used for this assignment.

### Available TLDs

OpenINTEL measures many TLDs. A subset are **open access** — no registration required:

| TLD | Since | Notes |
|-----|-------|-------|
| `.gov` | 2017-05-01 | US Federal government domains only (~11 k domains) |
| `.se` | 2016-06-07 | Sweden (~1.3 M domains) — good general-purpose TLD |
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
| `ds_key_tag` | INTEGER | Key tag identifying which DNSKEY the DS record references |
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

The table above is a curated subset. The full schema is roughly 100 columns; [openintel_csv/openintel_data_dictionary.md](openintel_csv/openintel_data_dictionary.md) lists every one of them alongside a per-`response_type` matrix showing which fields are actually populated for which record type — useful when deciding what to `.select()`. Regenerate it with `nids-spark-info.ipynb`.

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

See [Spark](Spark.md) for SparkSession setup, loading snapshots and longitudinal ranges, filtering/aggregating, and the `.toPandas()` rule (never call it on a large, unfiltered DataFrame — only on a final aggregated result).

> **TLD size guidance**: `.gov` (~11 k domains/day, ~7 MB) and `.li` (~85 k, ~42 MB) are ideal for development. `.se` (~1.3 M) and `.fr` (~4 M) are larger — these are better suited to running on a cluster (e.g., NRP's JupyterHub) rather than a laptop.

> **Which "total domains"?** Two populations are available and they are *not*
> interchangeable. Tasks 1 and 2 count **distinct NS `query_name`s**; Task 3
> counts **distinct SOA `query_name`s**, which is the larger of the two (on the
> `.se` 2024-01-15 snapshot: 1,331,863 vs 2,175,887 — a 63% gap, since the SOA
> population also covers `www.` labels). Use the denominator each task specifies
> and do not compare a Task 3 adoption rate against a Task 1/2 fraction directly.

---

## LACeS Anycast Census

The **LACeS Anycast Census** is a daily census of anycast-announced IPv4 (and IPv6) prefixes, produced by the University of Twente. It uses two detection methods:

- **AB (Anycast-Based)**: sends ICMP probes from many vantage points; a high response count indicates multiple instances, suggesting anycast.
- **GCD (Latency-Based)**: analyses RTT distributions from distributed vantage points to detect routing to different physical locations.

The census reports results at **/24 prefix granularity**. Each row represents a /24 prefix with its AB and GCD scores.

### Confidence Filtering

The live export reports AB and GCD scores **per probe method** rather than as
a single combined column each, so the documented filter is applied across the
`greatest()` of each family:

| Filter | Use case |
|--------|----------|
| `(max(AB_*) > 3) \| (max(GCD_*) > 1)` | **High confidence** — recommended for research |
| `(max(AB_*) > 1) \| (max(GCD_*) > 1)` | **Comprehensive** — maximises coverage, includes borderline cases |

### Loading the Census with Spark

The census is a small file (~a few MB), downloaded once to a local path and
loaded into Spark. See [Spark](Spark.md#7-broadcast-variables) for the full
loading + broadcast pattern used to cross-reference it against resolved NS
IPs in Task 2.

Full API documentation: [https://manycast.net/api/docs](https://manycast.net/api/docs)

### Census Schema

| Field | Type | Description |
|-------|------|-------------|
| `prefix` | STRING | /24 prefix string (e.g., `1.2.3.0/24`) |
| `AB_ICMPv4` | LONG | Anycast-Based score from ICMP probing (IPv4) — number of vantage points that replied |
| `AB_TCPv4` | LONG | Anycast-Based score from TCP probing (IPv4) |
| `AB_DNSv4` | LONG | Anycast-Based score from DNS probing (IPv4) |
| `GCD_ICMPv4` | LONG | Latency-based score from ICMP probing: **the number of distinct physical locations detected**, not a 0/1 flag. A value > 1 means anycast (e.g. `1.1.1.0/24` scores 74) |
| `GCD_TCPv4` | LONG | Latency-based score from TCP probing — same "count of locations" semantics |
| `partial` | BOOLEAN | True if the prefix appears only partially anycast |
| `backing_prefix` | STRING | The covering BGP prefix this /24 was observed inside |
| `ASN` | STRING | ASN(s) announcing the prefix. Note the uppercase name — Spark's default case-insensitive column resolution means `asn` also works |
| `locations` | ARRAY&lt;STRUCT&gt; | Detected instance locations: `city`, `country_code`, `id`, `lat`, `lon` |

> The `GCD_*` "count of locations" semantics are what make the `max(GCD_*) > 1`
> half of the confidence filter above meaningful — were these 0/1 flags, that
> term could never fire and the filter would silently collapse to its `AB` half.

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

[README](README.md) | [Introduction](Introduction.md) | Datasets ⮕ | [Spark](Spark.md) | [Tasks](Tasks.md) | [Task 1](Task-1-centralization.md) | [Task 2](Task-2-resilience.md) | [Task 3](Task-3-security.md) | [Notebook](nids-dns-ecosystem.ipynb)
