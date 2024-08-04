[![Cargo](https://img.shields.io/crates/v/redacter.svg)](https://crates.io/crates/redacter)
![tests and formatting](https://github.com/abdolence/redacter-rs/workflows/tests%20&amp;%20formatting/badge.svg)
![security audit](https://github.com/abdolence/redacter-rs/workflows/security%20audit/badge.svg)

# Redacter

Copy & Redact cli tool to securely copy and redact files across various sources and destinations,
utilizing Data Loss Prevention (DLP) capabilities.

## Features

* **Copy & Redact:**  copy files while applying DLP redaction to protect sensitive information.
* **Multiple Sources & Destinations:** interact with:
    * Local filesystem
    * Google Cloud Storage (GCS)
    * Amazon Simple Storage Service (S3)
    * Zip files
* **GCP DLP Integration:**  Leverage the power of GCP's DLP API for accurate and customizable redaction.
* **CLI:**  Easy-to-use command-line interface for streamlined workflows.
* Built with Rust to ensure speed, safety, and reliability.

## Installation

**Cargo:**

```sh
cargo install redacter
```

## Command line options

TBD

## Google authentication

Looks for credentials in the following places, preferring the first location found:

- A JSON file whose path is specified by the GOOGLE_APPLICATION_CREDENTIALS environment variable.
- A JSON file in a location known to the gcloud command-line tool using `gcloud auth application-default login`.
- On Google Compute Engine, it fetches credentials from the metadata server.

## Licence

Apache Software License (ASL)

## Author

Abdulla Abdurakhmanov
