[README](README.md) | Introduction ⮕ | [Datasets](Datasets.md) | [Tasks](Tasks.md) | [Task 1](Task-1-centralization.md) | [Task 2](Task-2-resilience.md) | [Task 3](Task-3-security.md) | [Notebook](nids-dns-ecosystem.ipynb)

# Introduction and Background

### Reading

- [OpenINTEL Project](https://openintel.nl) — the DNS measurement platform used in this module
- [OpenINTEL Zone-Based Measurements](https://openintel.nl/data/forward-dns/zone-based/) — technical background on the measurement methodology and how to download the original dataset.
- [OpenINTEL Data Dictionary](https://openintel.nl/data/dictionary/) — field-level description of all record types in the dataset
- [OpenINTEL-Spark - CAIDA.pdf](OpenINTEL-Spark%20-%20CAIDA.pdf) — guide to accessing OpenINTEL data via PySpark on the NRP JupyterHub environment
- [LACeS Anycast Census](https://manycast.net) — the anycast detection dataset used in Task 2

---

## The Domain Name System at Scale

The DNS is the Internet's naming infrastructure: it translates human-readable names (e.g., `www.example.com`) into the IP addresses that routers need to forward traffic. While often taken for granted, the DNS is a large, distributed, and operationally critical system. It is also a lens through which we can observe many structural properties of the Internet: who hosts what, how resilient infrastructure is, and how quickly security best practices are adopted.

At the top of the hierarchy sit the **root name servers**, which delegate authority for each Top-Level Domain (TLD) to the TLD's registry. The registry then maintains a **zone file** listing all registered domains under that TLD. For each domain, the zone file records which **authoritative name servers** (NS records) are responsible for answering queries.

OpenINTEL reads these zone files daily and actively queries every registered domain name, collecting a daily snapshot of the DNS state across hundreds of millions of domains. This module uses that data to study three aspects of the DNS ecosystem.

---

## Task 1 Background: DNS Hosting Centralization

When you register a domain, you choose a DNS hosting provider — the organisation that operates the authoritative name servers. A small registrar might run its own servers; a large operator might outsource to Cloudflare, AWS Route 53, Google, or another managed DNS provider.

The **NS records** in OpenINTEL data record the hostnames of these name servers (e.g., `ns1.cloudflare.com`, `ns-123.awsdns-45.com`). By extracting the **registrable domain** of each NS hostname — the `cloudflare.com` or `amazon.com` part — we can identify who operates that name server and measure the **market share** of each provider across all domains in the TLD.

### Why does concentration matter?

If many domains rely on the same DNS provider, that provider becomes a critical dependency for a large fraction of the Internet. The October 2016 Dyn DDoS attack is a famous example: because a significant fraction of high-profile websites used Dyn as their sole DNS provider, a single attack made those sites unreachable for millions of users worldwide. High market concentration creates systemic risk.

Measuring the **Herfindahl-Hirschman Index (HHI)** gives a single number summarising concentration: it is the sum of squared market shares, ranging from near 0 (many equal-sized providers) to 1.0 (one provider has 100% market share). Antitrust regulators typically flag markets with HHI > 0.25 as highly concentrated.

The rise of managed DNS services (Cloudflare launched its DNS in 2010; AWS Route 53 in 2010) means this market has likely become more concentrated over time. Longitudinal OpenINTEL data makes it possible to track this trend directly.

---

## Task 2 Background: DNS Infrastructure Resilience & Anycast

### NS Redundancy

RFC 1034 requires that every zone be served by at least two authoritative name servers, operated independently, so that the zone remains accessible if one server fails. In practice, many domains violate this requirement. Even when two NS records are present, both might be operated by the same provider — meaning a single provider outage takes down both.

Measuring NS redundancy has two dimensions:
1. **Server count**: how many distinct NS hostnames does a domain list?
2. **Provider diversity**: how many distinct providers (registrable domains) are behind those NS servers?

### Anycast in the DNS

**Anycast** is a routing technique where the same IP address is announced by multiple physical servers in different locations. BGP routes each client to the topologically closest instance, reducing latency and providing resilience: if one instance fails, BGP reconverges and routes clients to the next-closest one.

Anycast is fundamental to how root name servers and many large authoritative DNS providers achieve global scale and resilience. Cloudflare's `1.1.1.1` resolver and Google's `8.8.8.8` resolver both use anycast. Many authoritative providers (Cloudflare DNS, NS1, Google Cloud DNS) also anycast their authoritative servers.

To detect anycast, the **LACeS / Anycast Census** uses two complementary approaches:
- **Anycast-Based (AB)**: sends ICMP probes from many vantage points and counts how many receive replies — a high count suggests anycast.
- **Latency-Based (GCD)**: analyses RTT patterns from distributed vantage points to detect routing to different physical locations.

The census reports results at **/24 granularity** — i.e., it classifies each /24 prefix as anycast or unicast. By resolving NS hostnames to IP addresses and mapping each IP to its /24, we can determine whether a domain's name servers are anycast.

### SOA Parameters

The **SOA record** carries timing parameters that govern how secondary name servers behave:
- **refresh**: how often secondaries poll the primary for updates (typical: 3600–86400 s)
- **retry**: how long secondaries wait before re-polling after a failed refresh (typical: 600–3600 s)
- **expire**: how long secondaries keep serving the zone if they cannot reach the primary (typical: 604800–2592000 s / 1–4 weeks); if this is too short, a zone becomes unavailable during extended primary outages
- **minimum**: the default negative caching TTL for NXDOMAIN responses (RFC 2308 recommends ≤ 3600 s)

---

## Task 3 Background: DNS Security Best Practices

### DNSSEC

The DNS was designed without authentication: a resolver has no way to verify that a response it receives is legitimate. **DNSSEC** (DNS Security Extensions) adds cryptographic signatures to DNS responses, allowing resolvers to verify that data came from the authoritative server and has not been tampered with.

The **chain of trust** works as follows:
1. The root zone is signed; its DNSKEY is publicly known.
2. Each parent zone signs a **DS record** that references a DNSKEY in the child zone.
3. Resolvers walk the chain from root → TLD → domain, verifying signatures at each step.

A domain is effectively DNSSEC-secured only if *both* a DNSKEY is published *and* a DS record is present at the parent TLD. A domain with a DNSKEY but no DS record has published a key that no resolver will trust — it is not actually secured.

**DNSKEY algorithm** choice matters for security and performance:
- **RSA/SHA-256 (algorithm 8)**: historically dominant, computationally expensive
- **ECDSA P-256/SHA-256 (algorithm 13)**: widely recommended today — smaller keys, faster verification
- **Ed25519 (algorithm 15)**: modern, fast, small; adoption growing

### Sender Policy Framework (SPF)

SPF (RFC 7208) allows a domain owner to publish, via a TXT DNS record at the apex, a list of authorised mail senders. A receiving mail server checks whether the sending server's IP is in the domain's SPF record. SPF records follow the format:

```
v=spf1 include:_spf.google.com ip4:192.0.2.0/24 -all
```

The trailing **qualifier** sets the policy for unlisted senders:
- `-all`: **hard fail** — reject mail from unlisted servers (strongest)
- `~all`: **soft fail** — accept but mark suspicious
- `?all`: **neutral** — no policy
- `+all`: **pass all** — effectively useless (any server may send)

### CAA

CAA records (RFC 8659) allow domain owners to specify which Certificate Authorities (CAs) are permitted to issue TLS certificates for the domain. If a CA encounters a CAA record listing only specific CAs (e.g., `letsencrypt.org`), it must not issue a certificate unless it is listed. The `issue` tag restricts issuance; `issuewild` restricts wildcard issuance; a value of `;` prohibits all issuance from that CA.

[README](README.md) | Introduction ⮕ | [Datasets](Datasets.md) | [Tasks](Tasks.md) | [Task 1](Task-1-centralization.md) | [Task 2](Task-2-resilience.md) | [Task 3](Task-3-security.md) | [Notebook](nids-dns-ecosystem.ipynb)
