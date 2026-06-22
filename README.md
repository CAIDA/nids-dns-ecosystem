README ⮕ | [Introduction](Introduction.md) | [Datasets](Datasets.md) | [Tasks](Tasks.md) | [Task 1](Task-1-centralization.md) | [Task 2](Task-2-resilience.md) | [Task 3](Task-3-security.md) | [Notebook](nids-dns-ecosystem.ipynb)

# Understanding the DNS Ecosystem

## Learning Objectives

The goal of this assignment is to understand how the Domain Name System (DNS) is structured at Internet scale: who hosts it, how resilient it is to failures, whether anycast is deployed, and how broadly DNS-based security mechanisms have been adopted. You will work with active DNS measurement data from **OpenINTEL** project and active anycast detection data from the **Anycast Census** (LACeS) to answer these questions both for a single snapshot and longitudinally over several years.

## Overview

Start by reading **Introduction** to get the conceptual background. **Datasets** explains each dataset and how to access it.

- step 1 [read the introduction](Introduction.md)
- step 2 [read dataset overviews](Datasets.md)
- step 3 [review the tasks](Tasks.md)
- step 4 log into NRP's JupyterHub, upload and complete the nids-dns-ecosystem.ipynb
  - Detailed instructions to access NRP: [How to access NRP](https://www.caida.org/projects/nids/how-to/access-nrp/)
  - complete each task by replacing the `# YOUR CODE HERE` sections
  - answer all questions
- step 5 download your working notebook and replace ⬅ deliverable
- step 6 commit and push to github

### Directory Structure

```
nids-dns-ecosystem
├- Introduction.md                          # Introduction and background
├- Datasets.md                              # Dataset overview and access instructions
├- Tasks.md                                 # Task checklist and instructions
├- Task-1-centralization.md                 # Guidance for Task 1
├- Task-2-resilience.md                     # Guidance for Task 2
├- Task-3-security.md                       # Guidance for Task 3
├- data/                                    # Place downloaded OpenIntel parquet files here
├- nids-dns-ecosystem.ipynb             ⬅  # Complete / Commit / Push
```

### Glossary

- **Anycast**: A network addressing method where the same IP address is announced at multiple physical locations; BGP routes each client to the topologically closest instance. Widely used to distribute DNS servers globally.
- **Authoritative Name Server**: A DNS server that holds the definitive records for a zone and answers queries for it without consulting other servers.
- **CAA (Certification Authority Authorization)**: A DNS record type (RFC 8659) that restricts which Certificate Authorities are permitted to issue TLS certificates for a domain.
- **DNSKEY**: A DNS record type that publishes the public key used to verify DNSSEC signatures for a zone.
- **DNSSEC (DNS Security Extensions)**: A suite of IETF specifications (RFC 4033–4035) that adds cryptographic signatures to DNS responses, enabling resolvers to verify authenticity and integrity.
- **DS (Delegation Signer)**: A DNS record placed at the *parent* zone that contains a hash of a DNSKEY record in the *child* zone, establishing the DNSSEC chain of trust downward from the root.
- **LACeS / Anycast Census**: A longitudinal anycast detection system from the University of Twente that uses ICMP probing and latency analysis to identify anycast-announced /24 prefixes. Daily snapshots are available via [manycast.net](https://manycast.net). [[paper](https://doi.org/10.1145/3730567.3764484)]
- **NS (Name Server)**: A DNS record type that delegates authority for a zone to a set of authoritative name servers.
- **Registrable Domain**: The domain at one level below a public suffix — e.g., `cloudflare.com` extracted from `ns1.cloudflare.com`. Used here to identify which organisation operates a given nameserver.
- **SOA (Start of Authority)**: A mandatory DNS record type that stores administrative information about a zone, including timing parameters (refresh, retry, expire, minimum TTL) that govern secondary name server behaviour.
- **SPF (Sender Policy Framework)**: An email authentication mechanism (RFC 7208) published via TXT DNS records that specifies which mail servers are authorised to send email on behalf of a domain.
- **Zone File**: A text representation of all resource records in a DNS zone. Registries make zone files available; OpenIntel uses them as the seed list for its measurements.

README ⮕ | [Introduction](Introduction.md) | [Datasets](Datasets.md) | [Tasks](Tasks.md) | [Task 1](Task-1-centralization.md) | [Task 2](Task-2-resilience.md) | [Task 3](Task-3-security.md) | [Notebook](nids-dns-ecosystem.ipynb)
