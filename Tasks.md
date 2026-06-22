[README](README.md) | [Introduction](Introduction.md) | [Datasets](Datasets.md) | Tasks ⮕ | [Task 1](Task-1-centralization.md) | [Task 2](Task-2-resilience.md) | [Task 3](Task-3-security.md) | [Notebook](nids-dns-ecosystem.ipynb)

# Tasks

Complete the tasks below in order. All tasks are performed inside [nids-dns-ecosystem.ipynb](nids-dns-ecosystem.ipynb) — replace the `# YOUR CODE HERE` sections with your code and answer the questions in the markdown cells that follow each code cell.

---

## Task 0: Setup and Data Access

Download at least one OpenINTEL snapshot for `.se` (or `.gov` for a smaller first test) and place it in the `data/` directory. For the longitudinal tasks, download one snapshot per year (e.g., January 15th of each year, 2019–2024).

- [ ] Download an OpenINTEL snapshot from [data.openintel.nl](https://data.openintel.nl/download/forward-dns/basis=zonefile/)
- [ ] Run the setup cell in the notebook to install dependencies
- [ ] Verify that the SparkSession starts and can read the OpenINTEL data

---

## Task 1: DNS Hosting Centralization

Detailed guidance: [Task 1 — Centralization](Task-1-centralization.md)

### Task 1.1 — Load and inspect NS records

Load the OpenINTEL snapshot and extract NS records. Inspect the data to understand its structure.

### Task 1.2 — Extract DNS providers

Map each NS hostname to its **registrable domain** (the DNS hosting provider) using `tldextract`. Build a mapping from each domain to the set of providers it uses, and count how many domains each provider serves.

- [ ] **Q1** How many unique DNS hosting providers are there? What does the long tail of the CCDF suggest about the market structure?

### Task 1.3 — Market share and HHI

Compute each provider's market share (fraction of domains it serves). Compute the HHI. Print the top-10 providers.

- [ ] **Q2** What is the HHI? How does it compare to the monopoly bound (1.0) and to a perfectly even split across all providers (1/N)?
- [ ] **Q3** What fraction of all domains are served by the top-3 providers combined? What systemic risk does this represent?

### Task 1.4 — CCDF of domains per provider

Plot a log-log CCDF of the number of domains served per provider.

- [ ] **Q4** What does the shape of the CCDF tell you about the DNS hosting market? Is the distribution consistent with a power law?

### Task 1.5 — Longitudinal concentration trends

Repeat the market share and HHI computation for annual snapshots (e.g., 2019–2024). Plot HHI and top-3 share over time.

- [ ] **Q5** Has DNS hosting become more or less concentrated over the years? Which provider(s) drove the change?
- [ ] **Q6** Can you identify any events (product launches, acquisitions, major outages) that correlate with observed shifts in market share?

---

## Task 2: DNS Infrastructure Resilience & Anycast

Detailed guidance: [Task 2 — Resilience & Anycast](Task-2-resilience.md)

### Task 2.1 — NS server count per domain

Count the number of distinct NS hostnames per domain and plot the distribution.

- [ ] **Q7** What fraction of domains have fewer than 2 authoritative nameservers? What availability risk does a single nameserver create?

### Task 2.2 — DNS provider diversity

Using the provider mapping from Task 1, count how many distinct providers each domain uses. Plot the distribution.

- [ ] **Q8** What fraction of domains delegate to NS servers from only a single provider? If that provider became unavailable (as Dyn did in 2016), what fraction of domains would be unreachable?

### Task 2.3 — SOA parameter analysis

Extract SOA records and analyse the distribution of `expire`, `refresh`, and `minimum` values. Flag domains with values outside RFC-recommended ranges.

- [ ] **Q9** What fraction of domains have SOA `expire` values below one week (604800 s)? What does a very short expire value mean if the primary master is unreachable for several days?
- [ ] **Q10** What fraction of domains set the SOA `minimum` (negative caching TTL) above one day (86400 s)? Why might an excessively long negative TTL be problematic after a misconfiguration is fixed?

### Task 2.4 — Anycast cross-reference

Download the Anycast Census. Resolve the most common NS hostnames to IP addresses using `dnspython`. Map each IP to its /24 prefix and check whether it appears in the Anycast Census.

- [ ] **Q11** What fraction of unique NS server IP addresses fall within anycast-announced /24 prefixes? What fraction of domains have at least one anycast-backed nameserver?
- [ ] **Q12** Which DNS providers rely most heavily on anycast? Is there a correlation between a provider's market share (Task 1) and its use of anycast?

---

## Task 3: DNS Security Best Practices

Detailed guidance: [Task 3 — Security Best Practices](Task-3-security.md)

### Task 3.1 — DNSSEC adoption

Measure how many domains have a DS record (chain of trust established) vs. a DNSKEY record alone (key published but not linked to parent).

- [ ] **Q13** What fraction of domains are effectively DNSSEC-secured (DS record present)? What does it mean to have a DNSKEY but no DS record?

### Task 3.2 — DNSKEY algorithm distribution

Among DNSSEC-signed domains, count how many use each signing algorithm. Map algorithm IDs to names (RSA/SHA-256, ECDSA P-256, Ed25519, …).

- [ ] **Q14** What is the most common signing algorithm? What fraction of DNSSEC-signed domains still use legacy RSA-based algorithms vs. modern ECDSA or Ed25519?

### Task 3.3 — CAA adoption

Measure CAA adoption. Among domains with CAA records, identify the most commonly whitelisted Certificate Authorities.

- [ ] **Q15** What fraction of domains publish a CAA record? Which Certificate Authority is most commonly whitelisted? What fraction of CAA `issue` records prohibit all issuance (value = `";"`)?

### Task 3.4 — SPF adoption and policy strength

Find TXT records containing `v=spf1` and classify them by their trailing qualifier (`-all`, `~all`, `?all`, `+all`, or missing).

- [ ] **Q16** What fraction of domains publish an SPF record? What is the most common policy qualifier? What is the security implication of `+all` or a missing qualifier?

### Task 3.5 — Longitudinal adoption curves

Repeat the DNSSEC, CAA, and SPF adoption computation for annual snapshots. Plot all three on a single figure.

- [ ] **Q17** Which security mechanism has grown fastest? Are there any inflection points — years where adoption accelerated — and can you identify a plausible cause?
- [ ] **Q18** Is DNSSEC adoption correlated with CAA adoption at the domain level? Compute the overlap: what fraction of DNSSEC-signed domains also have a CAA record?

[README](README.md) | [Introduction](Introduction.md) | [Datasets](Datasets.md) | Tasks ⮕ | [Task 1](Task-1-centralization.md) | [Task 2](Task-2-resilience.md) | [Task 3](Task-3-security.md) | [Notebook](nids-dns-ecosystem.ipynb)
