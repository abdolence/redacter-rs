[![Cargo](https://img.shields.io/crates/v/redacter.svg)](https://crates.io/crates/redacter)
![tests and formatting](https://github.com/abdolence/redacter-rs/workflows/tests%20&amp;%20formatting/badge.svg)
![security audit](https://github.com/abdolence/redacter-rs/workflows/security%20audit/badge.svg)

# Redacter

Copy & Redact cli tool to securely copy and redact files across various sources and destinations,
utilizing Data Loss Prevention (DLP) capabilities.
The tool doesn't implement DLP itself, but rather relies on external models such as
Google Cloud Platform's DLP API.



![redacter-demo-ezgif com-video-to-gif-converter](https://github.com/user-attachments/assets/f6138848-7a2c-4c5e-9ad4-8ee2429f4cb7)




## Features

* **Copy & Redact:**  copy files while applying DLP redaction to protect sensitive information.
* **Multiple Sources & Destinations:** interact with:
    * Local filesystem
    * Google Cloud Storage (GCS)
    * Amazon Simple Storage Service (S3)
    * Zip files
* **DLP Integration:**
    * GCP DLP API for accurate and customizable redaction for:
        * text, html, json files
        * structured data table files (csv)
        * images (jpeg, png, bpm, gif)
* **CLI:**  Easy-to-use command-line interface for streamlined workflows.
* Built with Rust to ensure speed, safety, and reliability.

## Installation

### Binary releases

Download the latest release from [the GitHub releases](https://github.com/abdolence/redacter-rs/releases).

### Cargo

```sh
cargo install redacter
```

## Command line options

Copy and redact files from a source to a destination.

```
Usage: redacter cp [OPTIONS] <SOURCE> <DESTINATION>

Arguments:
  <SOURCE>       Source directory or file such as /tmp, /tmp/file.txt or gs://bucket/file.txt and others supported providers
  <DESTINATION>  Destination directory or file such as /tmp, /tmp/file.txt or gs://bucket/file.txt and others supported providers

Options:
  -m, --max-size-limit <MAX_SIZE_LIMIT>
          Maximum size of files to copy in bytes
  -f, --filename-filter <FILENAME_FILTER>
          Filter by name using glob patterns such as *.txt
  -d, --redact <REDACT>
          Redacter type [possible values: gcp-dlp]
      --gcp-project-id <GCP_PROJECT_ID>
          GCP project id that will be used to redact and bill API calls
      --allow-unsupported-copies
          Allow unsupported types to be copied without redaction
      --csv-headers-disable
          Disable CSV headers (if they are not present)
      --csv-delimiter <CSV_DELIMITER>
          CSV delimiter (default is ','
  -h, --help
          Print help
```

DLP is optional and should be enabled with `--redact` (`-d`) option.
Without DLP enabled, the tool will copy all files without redaction.
With DLP enabled, the tool will redact files based on the DLP model and skip unsupported files.

To be able to use GCP DLP you need to authenticate using `gcloud auth application-default login` or provide a service
account key using `GOOGLE_APPLICATION_CREDENTIALS` environment variable.

### Examples:

```sh
# Copy and redact a file from local filesystem to GCS
redacter cp -d gcp-dlp --gcp-project-id <your-gcp-project-with-dlp> sensitive.png gs://my-bucket-name/test/test.png  
```

The tool supports recursive copy of multiple files from directory:

```sh
redacter cp s3://my-bucket-name/sensitive-files/ tmp/
```

## Security considerations

- Your file contents are sent to the DLP API for redaction. Make sure you trust the DLP API provider.
- The accuracy of redaction depends on the DLP model, so don't rely on it as the only security measure.
- The tool was mostly design to redact files internally. Not recommended use it in public environments without proper
  security measures and manual review.
- Use it at your own risk. The author is not responsible for any data loss or security breaches.

## Licence

Apache Software License (ASL)

## Author

Abdulla Abdurakhmanov
