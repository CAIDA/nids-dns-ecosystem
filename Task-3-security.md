[README](README.md) | [Introduction](Introduction.md) | [Datasets](Datasets.md) | [Tasks](Tasks.md) | [Task 1](Task-1-centralization.md) | [Task 2](Task-2-resilience.md) | Task 3 ⮕ | [Notebook](nids-dns-ecosystem.ipynb)

# Task 3 Guidance: DNS Security Best Practices

This page provides implementation guidance for Task 3 of [nids-dns-ecosystem.ipynb](nids-dns-ecosystem.ipynb).

---

## DNSSEC Adoption

A domain is **effectively DNSSEC-secured** only when two conditions hold simultaneously:

1. The domain publishes a **DNSKEY** record (its public signing key).
2. The **parent zone** (the TLD registry) has published a **DS record** referencing that key, establishing the chain of trust from the root downwards.

In the OpenINTEL data, both conditions are measured at the registered domain level:

| Condition | How to detect in OpenINTEL |
|-----------|---------------------------|
| DNSKEY present | `response_type == 'DNSKEY'` row exists for `query_name` |
| DS present | `response_type == 'DS'` row exists for `query_name` |
| Properly secured | DS **and** DNSKEY present |
| Orphaned key | DNSKEY present, DS absent (resolver cannot validate — misconfiguration or broken chain) |

```python
ds_df     = df_snap.filter(psf.col("response_type") == "DS") \
                   .select("query_name", "ds_algorithm", "ds_key_tag").dropna(subset=["query_name"])
dnskey_df = df_snap.filter(psf.col("response_type") == "DNSKEY") \
                   .select("query_name", "dnskey_algorithm", "dnskey_flags").dropna(subset=["query_name"])

total = df_snap.filter(psf.col("response_type") == "SOA").select("query_name").distinct().count()

n_ds      = ds_df.select("query_name").distinct().count()
n_dnskey  = dnskey_df.select("query_name").distinct().count()

# Properly secured: has both DS and DNSKEY
n_secured = (
    ds_df.select("query_name").distinct()
         .join(dnskey_df.select("query_name").distinct(), "query_name", "inner")
         .count()
)
# Orphaned key: DNSKEY present, DS absent
n_orphan = (
    dnskey_df.select("query_name").distinct()
             .join(ds_df.select("query_name").distinct(), "query_name", "left_anti")
             .count()
)

print(f"Total domains:                 {total}")
print(f"Properly secured (DS+DNSKEY):  {n_secured} ({n_secured/total:.1%})")
print(f"Orphaned DNSKEY (no DS):       {n_orphan}  ({n_orphan/total:.1%})")
print(f"No DNSSEC:                     {total - n_dnskey} ({(total - n_dnskey)/total:.1%})")
```

---

## DNSKEY Algorithm IDs

The `dnskey_algorithm` field contains a numeric ID from the IANA DNS Security Algorithm Numbers registry. The most relevant ones:

| ID | Name | Notes |
|----|------|-------|
| 5 | RSA/SHA-1 | Deprecated; SHA-1 is cryptographically weak |
| 7 | RSASHA1-NSEC3-SHA1 | Deprecated |
| 8 | RSA/SHA-256 | Historically dominant; still widely used |
| 10 | RSA/SHA-512 | Rarely used |
| 13 | ECDSA P-256/SHA-256 | Recommended; smaller keys, faster verification |
| 14 | ECDSA P-384/SHA-384 | Higher security margin than P-256 |
| 15 | Ed25519 | Modern, fast, small; growing adoption |
| 16 | Ed448 | Higher security than Ed25519; very rare |

**Key Signing Keys (KSK)** have `dnskey_flags == 257`; **Zone Signing Keys (ZSK)** have `dnskey_flags == 256`. Use the KSK algorithm when summarising a zone's signing algorithm choice, as the KSK is what the DS record references.

```python
DNSKEY_ALGORITHM_NAMES = {
    5: "RSA/SHA-1 (5)", 7: "RSASHA1-NSEC3 (7)", 8: "RSA/SHA-256 (8)",
    10: "RSA/SHA-512 (10)", 13: "ECDSA P-256 (13)", 14: "ECDSA P-384 (14)",
    15: "Ed25519 (15)", 16: "Ed448 (16)",
}

# Use KSK (flags == 257) to characterise each zone's algorithm choice
algo_counts_pd = (
    dnskey_df.filter(psf.col("dnskey_flags") == 257)
             .groupBy("dnskey_algorithm")
             .agg(psf.countDistinct("query_name").alias("domain_count"))
             .orderBy(psf.col("domain_count").desc())
             .toPandas()
)
algo_counts_pd["algo_name"] = algo_counts_pd["dnskey_algorithm"].map(
    lambda x: DNSKEY_ALGORITHM_NAMES.get(int(x), f"Other ({int(x)})")
)
total_signed = algo_counts_pd["domain_count"].sum()
algo_counts_pd["fraction"] = algo_counts_pd["domain_count"] / total_signed
print(algo_counts_pd[["algo_name", "domain_count", "fraction"]].to_string(index=False))
```

---

## CAA Adoption

CAA records use three tags:

| Tag | Meaning |
|-----|---------|
| `issue` | Which CAs may issue standard certificates |
| `issuewild` | Which CAs may issue wildcard certificates |
| `iodef` | Where to report policy violations |

The `caa_value` field holds the CA's domain (e.g., `letsencrypt.org`, `digicert.com`) or `;` to prohibit issuance entirely from that CA class.

```python
caa_df = df_snap.filter(psf.col("response_type") == "CAA") \
                .select("query_name", "caa_tag", "caa_value").dropna(subset=["query_name"])

n_caa = caa_df.select("query_name").distinct().count()
print(f"CAA adoption: {n_caa / total:.1%}")

# Most common CAs in 'issue' records
ca_counts_pd = (
    caa_df.filter(psf.col("caa_tag") == "issue")
          .withColumn("caa_value", psf.lower(psf.trim(psf.col("caa_value"))))
          .groupBy("caa_value")
          .agg(psf.countDistinct("query_name").alias("domain_count"))
          .orderBy(psf.col("domain_count").desc())
          .limit(10)
          .toPandas()
)
print(ca_counts_pd.to_string(index=False))
```

---

## SPF Parsing

SPF records are published as TXT records at the domain apex. A TXT row may contain multiple concatenated string values — OpenINTEL stores them joined in `txt_text`. An SPF record always starts with `v=spf1`.

### Extracting SPF

```python
# spf_qualifier_udf is defined in the notebook setup cell
spf_df = (
    df_snap.filter(psf.col("response_type") == "TXT")
           .filter(psf.col("txt_text").rlike("(?i)v=spf1"))
           .withColumn("qualifier", spf_qualifier_udf(psf.col("txt_text")))
)

n_spf = spf_df.select("query_name").distinct().count()
print(f"SPF adoption: {n_spf / total:.1%}")

qualifier_dist = (
    spf_df.groupBy("qualifier")
          .agg(psf.countDistinct("query_name").alias("count"))
          .withColumn("fraction", psf.col("count") / n_spf)
          .orderBy(psf.col("count").desc())
)
qualifier_dist.show()
```

### Security implications of each qualifier

| Qualifier | Policy | Security |
|-----------|--------|----------|
| `-all` | Hard fail — unlisted senders rejected | Strongest — actively blocks spoofed mail |
| `~all` | Soft fail — unlisted senders accepted but marked | Moderate — helps filtering but does not reject |
| `?all` | Neutral — no policy for unlisted | Weak — provides no protection |
| `+all` | Pass all — any server may send | None — effectively disables SPF |
| (no `all`) | Incomplete record | None — receiving servers may not enforce SPF |

---

## Longitudinal Adoption Curves

Because `df_long` already contains a `year` column from the partition path, a single Spark job computes all three mechanisms across all years:

```python
YEARS   = [2019, 2020, 2021, 2022, 2023, 2024]
df_long = load_longitudinal(SOURCE, YEARS)

# Totals per year
total_per_year_pd = (
    df_long.filter(psf.col("response_type") == "SOA")
           .groupBy("year").agg(psf.count("query_name").alias("total"))
           .toPandas().set_index("year")
)

def adoption_per_year(df_long, rtype, filter_expr=None):
    f = df_long.filter(psf.col("response_type") == rtype)
    if filter_expr is not None:
        f = f.filter(filter_expr)
    return (
        f.groupBy("year")
         .agg(psf.countDistinct("query_name").alias("count"))
         .toPandas().set_index("year")
    )

dnssec_pd = adoption_per_year(df_long, "DS")
caa_pd    = adoption_per_year(df_long, "CAA")
spf_pd    = adoption_per_year(df_long, "TXT", psf.col("txt_text").rlike("(?i)v=spf1"))

# Plot
fig, ax = plt.subplots(figsize=(9, 5))
for label, counts_pd in [("DNSSEC", dnssec_pd), ("CAA", caa_pd), ("SPF", spf_pd)]:
    rates = counts_pd["count"] / total_per_year_pd["total"]
    ax.plot(rates.index, rates.values, marker='o', label=label)

ax.set_xlabel("Year")
ax.set_ylabel("Adoption rate")
ax.set_title(f"DNS security best practice adoption (.{SOURCE})")
ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: f"{y:.0%}"))
ax.legend()
ax.grid(True, linestyle="--", alpha=0.4)
plt.tight_layout()
```

### DNSSEC × CAA overlap

```python
ds_names  = df_snap.filter(psf.col("response_type") == "DS").select("query_name").distinct()
caa_names = caa_df.select("query_name").distinct()

n_both = ds_names.join(caa_names, "query_name", "inner").count()
print(f"Domains with DNSSEC + CAA: {n_both} ({n_both/n_ds:.1%} of DNSSEC-signed domains)")
```

[README](README.md) | [Introduction](Introduction.md) | [Datasets](Datasets.md) | [Tasks](Tasks.md) | [Task 1](Task-1-centralization.md) | [Task 2](Task-2-resilience.md) | Task 3 ⮕ | [Notebook](nids-dns-ecosystem.ipynb)
