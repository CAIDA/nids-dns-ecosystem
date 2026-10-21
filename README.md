README ⮕ | [Introduction](Introduction.md) | [Datasets](Datasets.md) | [Spark](Spark.md) | [Tasks](Tasks.md) | [Task 1 Guide](Task-1-centralization.md) | [Task 2 Guide](Task-2-resilience.md) | [Task 3 Guide](Task-3-security.md) | [Notebook](nids-dns-ecosystem.ipynb)

# Understanding the DNS Ecosystem

## Learning Objectives

The goal of this assignment is to understand how the Domain Name System (DNS) is structured at Internet scale: who hosts it, how resilient it is to failures, whether anycast is deployed, and how broadly DNS-based security mechanisms have been adopted. You will work with active DNS measurement data from **OpenINTEL** project and active anycast detection data from the **Anycast Census** (LACeS) to answer these questions both for a single snapshot and longitudinally over several years.

## Slides

- [ETP week 05 DNS](slides/ETP-Week-05-DNS.pptx)

## Overview

Start by reading **Introduction** to get the conceptual background. **Datasets** explains each dataset and how to access it.

- step 1 [read the introduction](Introduction.md)
- step 2 [read dataset overviews](Datasets.md)
- step 3 [read the Spark guide](Spark.md)
- step 4 [review the tasks](Tasks.md)
- step 5 set the module up on the hub (see "Running this module" below)
- step 6 launch Jupyter and complete nids-dns-ecosystem.ipynb
  - complete each task by replacing the `# YOUR CODE HERE` sections
  - set `SOURCE = "se"` for your final run — `.se` is the graded TLD
  - answer all questions
- step 7 download your working notebook and replace ⬅ deliverable
- step 8 commit and push to github

### Running this module

This module runs on the NRP JupyterHub — [How to access NRP](https://www.caida.org/projects/nids/how-to/access-nrp/)
covers getting an account and a namespace.

**At spawn, select the `Pyspark` image.** This module's queries run under Spark, which needs a Java
runtime; the default preset image does not have one, and picking it fails at the first Spark cell
with `JAVA_HOME is not set`. The image is fixed once the server starts, so if you pick the wrong
one, stop the server from the JupyterHub control panel and start a new one.

Its environment is built by [`nids-setup`](https://github.com/CAIDA/nids-setup), the same tool that
sets up every other NIDS module — from a terminal inside your notebook server:

```bash
git clone https://github.com/CAIDA/nids-setup.git     # beside this repo
cd nids-setup
./setup.sh --nrp --release all --assignment DNS
```

On the hub the image already supplies the packages, so this clones the module rather than building
an environment. `--release all` is needed because this module ships in the `later` tier, not in
`r1`. Once that is done:

```bash
jupyter lab nids-dns-ecosystem/nids-dns-ecosystem.ipynb
```

No data download is required. The notebook reads OpenINTEL's Parquet directly from the public
`openintel-public` object store over S3A, anonymously and with no credentials — see
[Datasets](Datasets.md#accessing-the-data-via-spark). Spark resolves its S3A jars from Maven
Central the first time a session starts, which makes the first run slower than later ones.

### Directory Structure

```
nids-dns-ecosystem
├- Introduction.md                          # Introduction and background
├- Datasets.md                              # Dataset overview and access instructions
├- Spark.md                                 # PySpark building blocks for the queries
├- Tasks.md                                 # Task checklist and instructions
├- Task-1-centralization.md                 # Guidance for Task 1
├- Task-2-resilience.md                     # Guidance for Task 2
├- Task-3-security.md                       # Guidance for Task 3
├- openintel_csv/                           # Generated schema / partition-size reference
├- slides/                                  # Lecture slides
├- OpenINTEL-Spark - CAIDA.pdf              # OpenINTEL data and Spark presentation
├- nids-dns-ecosystem.ipynb             ⬅  # Complete / Commit / Push
```

OpenINTEL data is read directly from a public S3 object store — there is nothing
to download by hand and no credentials to configure. The one local download is
the Anycast Census, which the notebook fetches to `/tmp` for you.

### Glossary

- **Anycast**: A network addressing method where the same IP address is announced at multiple physical locations; BGP routes each client to the topologically closest instance. Widely used to distribute DNS servers globally.
- **Authoritative Name Server**: A DNS server that holds the definitive records for a zone and answers queries for it without consulting other servers.
- **CAA (Certification Authority Authorization)**: A DNS record type (RFC 8659) that restricts which Certificate Authorities are permitted to issue TLS certificates for a domain.
- **DNSKEY**: A DNS record type that publishes the public key used to verify DNSSEC signatures for a zone.
- **DNSSEC (DNS Security Extensions)**: A suite of IETF specifications (RFC 4033–4035) that adds cryptographic signatures to DNS responses, enabling resolvers to verify authenticity and integrity.
- **DS (Delegation Signer)**: A DNS record placed at the _parent_ zone that contains a hash of a DNSKEY record in the _child_ zone, establishing the DNSSEC chain of trust downward from the root.
- **LACeS / Anycast Census**: A longitudinal anycast detection system from the University of Twente that uses ICMP probing and latency analysis to identify anycast-announced /24 prefixes. Daily snapshots are available via [manycast.net](https://manycast.net). [[paper](https://doi.org/10.1145/3730567.3764484)]
- **NS (Name Server)**: A DNS record type that delegates authority for a zone to a set of authoritative name servers.
- **Registrable Domain**: The domain at one level below a public suffix — e.g., `cloudflare.com` extracted from `ns1.cloudflare.com`. Used here to identify which organisation operates a given nameserver.
- **SOA (Start of Authority)**: A mandatory DNS record type that stores administrative information about a zone, including timing parameters (refresh, retry, expire, minimum TTL) that govern secondary name server behaviour.
- **SPF (Sender Policy Framework)**: An email authentication mechanism (RFC 7208) published via TXT DNS records that specifies which mail servers are authorised to send email on behalf of a domain.
- **Zone File**: A text representation of all resource records in a DNS zone. Registries make zone files available; OpenINTEL uses them as the seed list for its measurements.

README ⮕ | [Introduction](Introduction.md) | [Datasets](Datasets.md) | [Spark](Spark.md) | [Tasks](Tasks.md) | [Task 1 Guide](Task-1-centralization.md) | [Task 2 Guide](Task-2-resilience.md) | [Task 3 Guide](Task-3-security.md) | [Notebook](nids-dns-ecosystem.ipynb)
