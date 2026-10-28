[![Cargo](https://img.shields.io/crates/v/redacter.svg)](https://crates.io/crates/redacter)
![tests and formatting](https://github.com/abdolence/redacter-rs/workflows/tests%20&amp;%20formatting/badge.svg)
![security audit](https://github.com/abdolence/redacter-rs/workflows/security%20audit/badge.svg)

# Redacter

Detect & Redact cli tool to find Personal Identifiable Information (PII) in files, and to securely copy and redact
files removing it, across various sources and destinations and utilising Data Leak/Loss Prevention (DLP) capabilities.
`redacter cp` copies files and redacts them on the way; `redacter scan` only reads them and reports what was found and
where, with an exit code a CI pipeline can gate on.

The tool ships three redacters that run entirely on your machine, a rule-based one with checksum validation, a small
multilingual named-entity model and a zero-shot span model that looks for labels you choose, and can also delegate to
cloud DLP services and LLMs such as Google Cloud Platform's DLP API. Local redaction keeps the data on the host and
costs nothing per file; the cloud providers find more. See [Local or
cloud](#local-or-cloud-quality-versus-performance) for the measured trade-off.

![redacter-demo](media/redacter-demo.gif)

## Features

* **Copy & Redact:**  copy files while applying DLP redaction to protect sensitive information.
* **Scan:** detect sensitive information in files without changing them, with the same providers, and report what was
  found and where as text or JSON. See [Scan command](#scan-command).
* **Multiple Sources & Destinations:** interact with:
    * Local filesystem
    * Google Cloud Storage (GCS)
    * Amazon Simple Storage Service (S3)
    * Zip files
    * Clipboard (text content and images)
* **DLP Integration:**
    * [Google Cloud Platform DLP](https://cloud.google.com/security/products/dlp?hl=en) for accurate and customizable
      redaction for:
        * text, html, json files
        * structured data table files (csv)
        * images (jpeg, png, bpm, gif)
        * PDF files (rendering as images)
    * [Microsoft Presidio](https://microsoft.github.io/presidio/) for PII redaction (open source project that you can
      install on-prem).
        * text, html, csv, json files
        * images
        * PDF files (rendering as images)
    * [GCP Vertex AI](https://cloud.google.com/vertex-ai/docs) based redaction using any available models such as
      Gemini, Claude, etc.:
        * text, html, csv, json files
        * images, redacted by the model itself or by blacking out the coordinates it reports
        * PDF files (rendering as images)
    * [Google Gemini API](https://ai.google.dev/) based redaction
        * text, html, csv, json files
        * images, redacted by the model itself or by blacking out the coordinates it reports
        * PDF files (rendering as images)
    * [Open AI LLM](https://openai.com/) based redaction
        * text, html, csv, json files
        * images, redacted by the model itself or by blacking out the coordinates it reports
        * PDF files (rendering as images)
    * [AWS Comprehend](https://aws.amazon.com/comprehend/) PII redaction:
        * text, html, csv, json files
        * images through text extraction using OCR
        * PDF files (rendering as images from OCR)
    * Local rules redacter: offline regex and checksum based redaction of emails, phone numbers,
      payment cards, IBANs, network addresses, secrets, national identifiers of 43 countries,
      postcodes and dates of birth, plus your own regex and dictionary rules. No cloud account needed.
    * Local NER redacter: offline detection of people, organisations and locations with a
      multilingual transformer model running on your CPU. No cloud account needed:
        * text, html, csv, json files
        * images through text extraction using OCR
        * PDF files (rendering as images from OCR)
    * Local GLiNER redacter: offline, zero-shot span detection against free-text labels you
      choose, understanding context that `local-ner` cannot. No cloud account needed:
        * text, html, csv, json files
        * images through text extraction using OCR
        * PDF files (rendering as images from OCR)
    * [AWS Bedrock](https://aws.amazon.com/bedrock/) based redaction using Amazon Nova and other models available on Bedrock
        * text, html, csv, json files
        * images, redacted by blacking out the coordinates the model reports
        * PDF files (rendering as images)
    * [AWS Bedrock Guardrails](https://aws.amazon.com/bedrock/guardrails/) based redaction
        * text, html, csv, json files
    * ... more DLP providers can be added in the future.
* **CLI:**  Easy-to-use command-line interface for streamlined workflows.
* Built with Rust to ensure speed, safety, and reliability.

## Installation

### Binary releases

Download the latest release from [the GitHub releases](https://github.com/abdolence/redacter-rs/releases).

### Cargo

```sh
cargo install redacter
```

### Optional capabilities installation

PDF redaction, OCR (for the DLP providers that can't read images) and the `local-ner` and `local-gliner`
redacters need addons that are not bundled with the tool. Install the ones you need with
`redacter addons install`, see [Addons](#addons).

## Command line options

Copy and redact files from a source to a destination.

```
Usage: redacter cp [OPTIONS] <SOURCE> <DESTINATION>

Arguments:
  <SOURCE>
          Source directory or file such as /tmp, /tmp/file.txt or gs://bucket/file.txt and others supported providers

  <DESTINATION>
          Destination directory or file such as /tmp, /tmp/file.txt or gs://bucket/file.txt and others supported providers

Options:
  -m, --max-size-limit <MAX_SIZE_LIMIT>
          Maximum size of files to copy in bytes

      --addons-dir <ADDONS_DIR>
          Directory holding the addons that `addons install` installs and manually placed copies, one subdirectory per addon. Overrides the REDACTER_ADDONS_DIR environment variable. Default is the user cache directory, for example ~/.cache/redacter/addons

  -n, --max-files-limit <MAX_FILES_LIMIT>
          Maximum number of files to copy. Sort order is not guaranteed and depends on the provider

  -f, --filename-filter <FILENAME_FILTER>
          Filter by name using glob patterns such as *.txt

  -d, --redact <REDACT>
          List of redacters to use
          
          [possible values: gcp-dlp, aws-comprehend, ms-presidio, gemini-llm, open-ai-llm, gcp-vertex-ai, aws-bedrock, aws-bedrock-guardrails, local-rules, local-ner, local-gliner]

      --allow-unsupported-copies
          Allow unsupported types to be copied without redaction

      --gcp-project-id <GCP_PROJECT_ID>
          GCP project id that will be used to redact and bill API calls

      --gcp-dlp-built-in-info-type <GCP_DLP_BUILT_IN_INFO_TYPE>
          Additional GCP DLP built in info types for redaction

      --gcp-dlp-stored-info-type <GCP_DLP_STORED_INFO_TYPE>
          Additional GCP DLP user defined stored info types for redaction

      --llm-image-mode <LLM_IMAGE_MODE>
          How LLM redacters redact images: 'native' lets the model edit the image, 'coords' asks the model for coordinates and blacks them out locally, 'auto' edits natively then verifies the edit with the coordinate pass, falling back to coordinates entirely when the model cannot edit images

          Possible values:
          - auto:   Edit the image with the model, verify the edit with the coordinate pass, and fall back to coordinates entirely when the model cannot edit images
          - native: Always edit the image with the model, failing when it cannot
          - coords: Always ask the model for PII coordinates and black them out locally
          
          [default: auto]

      --gcp-region <GCP_REGION>
          GCP location for Vertex AI. Default is 'global'; 'us' and 'eu' multi-regions and regional locations such as 'us-central1' are accepted

      --gcp-vertex-ai-text-model <GCP_VERTEX_AI_TEXT_MODEL>
          Model name for text redaction in Vertex AI, also used to locate PII coordinates in images. Default is 'publishers/google/models/gemini-3.8-flash'

      --gcp-vertex-ai-image-model <GCP_VERTEX_AI_IMAGE_MODEL>
          Model name for native image editing in Vertex AI. Default is 'publishers/google/models/gemini-3.1-flash-image'

      --gcp-vertex-ai-block-none-harmful
          Block none harmful content threshold for Vertex AI redacter. Default is BlockOnlyHigh since BlockNone is required a special billing settings.

      --csv-headers-disable
          Disable CSV headers (if they are not present)

      --csv-delimiter <CSV_DELIMITER>
          CSV delimiter (default is ',')

      --aws-region <AWS_REGION>
          AWS region for the AWS Comprehend and AWS Bedrock redacters

      --aws-bedrock-text-model <AWS_BEDROCK_TEXT_MODEL>
          Bedrock model id for text redaction, also used to locate PII coordinates in images and, since Bedrock has no active image editing model, to redact images. Default is 'amazon.nova-2-lite-v1:0' with the inference profile prefix of the selected region ('us.', 'eu.' or 'jp.'), falling back to the 'global.' profile elsewhere

      --aws-bedrock-guardrail-id <AWS_BEDROCK_GUARDRAIL_ID>
          Guardrail id for the AWS Bedrock Guardrails redacter

      --aws-bedrock-guardrail-version <AWS_BEDROCK_GUARDRAIL_VERSION>
          Guardrail version for the AWS Bedrock Guardrails redacter. Default is 'DRAFT'

      --ms-presidio-text-analyze-url <MS_PRESIDIO_TEXT_ANALYZE_URL>
          URL for text analyze endpoint for MsPresidio redacter

      --ms-presidio-image-redact-url <MS_PRESIDIO_IMAGE_REDACT_URL>
          URL for image redact endpoint for MsPresidio redacter

      --gemini-model <GEMINI_MODEL>
          Gemini model name for text redaction, also used to locate PII coordinates in images. Default is 'models/gemini-3.8-flash'

      --gemini-image-model <GEMINI_IMAGE_MODEL>
          Gemini model name for native image editing. Default is 'models/gemini-3.1-flash-image'

      --sampling-size <SAMPLING_SIZE>
          Sampling size in bytes before redacting files. Disabled by default

      --open-ai-api-key <OPEN_AI_API_KEY>
          API key for OpenAI LLM redacter

      --open-ai-model <OPEN_AI_MODEL>
          Open AI chat model name for text redaction, also used to locate PII coordinates in images. Default is 'gpt-5.6-luna'

      --open-ai-image-model <OPEN_AI_IMAGE_MODEL>
          Open AI model name for native image editing. Default is 'gpt-image-2'

      --limit-dlp-requests <LIMIT_DLP_REQUESTS>
          Limit the number of DLP requests. Some DLPs has strict quotas and to avoid errors, limit the number of requests delaying them. Default is disabled

      --local-rules <LOCAL_RULES>
          Rule groups enabled for the local-rules redacter, comma separated. Default is every group
          
          [possible values: email, phone, payment-card, iban, network, url, secrets, us-identifiers, eu-identifiers, world-identifiers, postcodes, birth-dates, custom]

      --local-rules-disable <LOCAL_RULES_DISABLE>
          Rule groups disabled for the local-rules redacter, comma separated. Applied after --local-rules
          
          [possible values: email, phone, payment-card, iban, network, url, secrets, us-identifiers, eu-identifiers, world-identifiers, postcodes, birth-dates, custom]

      --local-rule <LOCAL_RULE>
          User-defined regex rule for the local-rules redacter in the form name=regex. Can be repeated

      --local-rules-file <LOCAL_RULES_FILE>
          JSON file with user-defined regex and dictionary rules for the local-rules redacter. Can be repeated

      --local-ner-entities <LOCAL_NER_ENTITIES>
          Entity types the local-ner redacter removes, comma separated: per (people), org (organisations), loc (locations). Default is all three
          
          [possible values: per, org, loc]

      --local-ner-min-score <LOCAL_NER_MIN_SCORE>
          Minimum model confidence for a word to be redacted by local-ner, between 0 and 1. Lower values redact more. Default is 0.5
          
          [default: 0.5]

      --local-gliner-labels <LOCAL_GLINER_LABELS>
          Entity labels the local-gliner redacter looks for, comma separated, free text understood by the model (for example "person,email,medical condition"). Default is a PII list: person, address, email, phone number, date of birth, passport number, national id number, credit card number, iban, ip address, username, medical condition, postal code, license plate number

      --local-gliner-min-score <LOCAL_GLINER_MIN_SCORE>
          Minimum model confidence for a span to be redacted by local-gliner, between 0 and 1. Lower values redact more. Default is 0.5
          
          [default: 0.5]

      --mime-override <MIME_OVERRIDE>
          Override media type detection using glob patterns such as 'text/plain=*.md'

      --report-file <REPORT_FILE>
          Write the report to this file

      --report-format <REPORT_FORMAT>
          Format of the report file: 'json' for machines, 'text' for people. Requires --report-file. Default is 'json'
          
          [default: json]
          [possible values: json, text]

  -h, --help
          Print help (see a summary with '-h')
```

DLP is optional and should be enabled with `--redact` (`-d`) option.
Without DLP enabled, the tool will copy all files without redaction.
With DLP enabled, the tool will redact files based on the DLP model and skip unsupported files.

`--report-file` saves the result of the command to a file: `files_copied`, `files_redacted` and `files_skipped`
as JSON by default, or the same three counts as text with `--report-format text`. The terminal output is the same
with or without it. `--report-file` and `--report-format` replace `--save-json-results` of earlier versions, which
is no longer accepted. `scan` takes the same two options, see [Scan command](#scan-command).

## Source and destinations

Source/destination can be a local file or directory, or a file in GCS, S3, and others:

- Local file: `/tmp/file.txt` or `/tmp` for whole directory recursive copy
- GCS: `gs://bucket/file.txt` or `gs://bucket/test-dir/` for whole directory recursive copy
- S3: `s3://bucket/file.txt` or `s3://bucket/test-dir/` for whole directory recursive copy
- Zip archive: `zip://tmp/archive.zip`
- Clipboard: `clipboard://`

## DLP and redacters

### Google Cloud Platform DLP

To be able to use GCP DLP you need to:

- authenticate using `gcloud auth application-default login` or provide a service account key
  using `GOOGLE_APPLICATION_CREDENTIALS` environment variable.
- provide a GCP project id using `--gcp-project-id` option.

Additionally you can provide the list of user defined info types using `--gcp-dlp-stored-info-type` option.

### Microsoft Presidio

To be able to use Microsoft Presidio DLP you need to have a running instance of the Presidio API.
You can use Docker to run it locally or deploy it to your infrastructure.
You need to provide the URLs for text analysis and image redaction endpoints using `--ms-presidio-text-analyze-url` and
`--ms-presidio-image-redact-url` options.

### GCP Vertex AI

Vertex AI redacter supports any available models on GCP Vertex AI Models Garden, such as:

- Google Gemini
- Claude
- etc.

Default models are set to Gemini models.

To be able to use GCP Vertex AI you need to:

- authenticate using `gcloud auth application-default login` or provide a service account key
  using `GOOGLE_APPLICATION_CREDENTIALS` environment variable.
- provide a GCP project id using `--gcp-project-id` option.

The location is optional and defaults to `global`. Use `--gcp-region` to pick another one: the `us` and `eu`
multi-regions and regional locations such as `us-central1` are accepted. Note that the Gemini 3.x models are
served only on `global`, `us` and `eu`.

You can specify different models using `--gcp-vertex-ai-text-model` and `--gcp-vertex-ai-image-model` options.
By default, they are set to:

- `publishers/google/models/gemini-3.8-flash` for the text model
- `publishers/google/models/gemini-3.1-flash-image` for the image model

### Google Gemini API

To be able to use the Gemini API redacter you need to authenticate with credentials that carry the
`https://www.googleapis.com/auth/generative-language` scope and provide a GCP project id using `--gcp-project-id`.
Models are selected with `--gemini-model` (default `models/gemini-3.8-flash`) and `--gemini-image-model`
(default `models/gemini-3.1-flash-image`).

### Open AI LLM

To be able to use Open AI LLM you need to provide an API key using `--open-ai-api-key` command line option.
Optionally, you can provide model names using `--open-ai-model` (default `gpt-5.6-luna`) and
`--open-ai-image-model` (default `gpt-image-2`) options.

### Image redaction with LLM redacters

GCP Vertex AI, Gemini API and Open AI redact images in one of two ways, selected with
`--llm-image-mode`; AWS Bedrock has no active image editing model and always redacts images by
coordinates, so `--llm-image-mode native` is rejected for it:

- `native` (the image model edits the image and returns it with the personal information covered by black boxes);
- `coords` (the text model reports the coordinates of the personal information and the tool blacks them out locally);
- `auto` (the default): the native path is tried first and its result is then verified by running the coordinate
  path over the edited image, blacking out anything the model left legible; the tool falls back to coordinates
  entirely when the model cannot edit images. Authentication, permission and quota errors are never retried.

### AWS Comprehend

To be able to use AWS Comprehend DLP you need to authenticate using `aws configure` or provide a service account.
To provide an AWS region use `--aws-region` option since AWS Comprehend may not be available in all regions.
AWS Comprehend DLP is only available for unstructured text files.

### AWS Bedrock

To be able to use AWS Bedrock you need to authenticate using `aws login`, `aws configure` or a service account,
and have model access enabled for the models you use in the Bedrock console.
To provide an AWS region use `--aws-region` option.

The text model is used for text redaction and to locate PII coordinates in images; AWS Bedrock has no active
image editing model, so images are always redacted by that coordinate path (`--llm-image-mode native` is
rejected for this redacter). Select the model with `--aws-bedrock-text-model`; by default it is
`amazon.nova-2-lite-v1:0`. Amazon Nova 2 Lite has no in-region endpoint and is served only through Geo
inference profiles, so the default id is prefixed with the geography of the selected region (`us.`, `eu.`, or
`jp.` for `ap-northeast-1`/`ap-northeast-3`). Outside the US, EU and Japan the default falls back to the
`global.` profile, which may route the request to another geography; pass an explicit id to
`--aws-bedrock-text-model` to override it.

Text redaction works with any Converse-capable model id. Images are redacted with Amazon Nova (the default) or
Anthropic Claude models, detected from the model id; any other model family is rejected for image redaction
with an error rather than risking misplaced boxes.

### AWS Bedrock Guardrails

This redacter sends text to the [`ApplyGuardrail`](https://aws.amazon.com/bedrock/guardrails/) API of a guardrail
you own, and returns the anonymized text the service produces. Placeholders such as `{NAME}`, `{EMAIL}` or
`{PHONE}` that the guardrail inserts in place of the sensitive values are kept as-is in the output. Only text-based
formats are supported (text, html, csv, json); images and PDFs are not.

To be able to use it you need to authenticate the same way as for AWS Bedrock (`aws login`, `aws configure` or a
service account), provide an AWS region using `--aws-region`, and pass the id of a guardrail you own using
`--aws-bedrock-guardrail-id`. Use `--aws-bedrock-guardrail-version` to pick a specific published version; the
default is `DRAFT`.

The guardrail's sensitive information filters must be configured with the action **Anonymize**. If a filter is
instead configured to **Block**, the service returns the guardrail's blocked message instead of redacted text, and
the tool rejects the response rather than returning that message as if it were redacted content.

You can create a guardrail with its PII filters set to anonymize using the AWS CLI. For example, with a
`guardrail-pii.json` file such as:

```json
{
  "piiEntitiesConfig": [
    {"type": "NAME", "action": "ANONYMIZE", "inputAction": "ANONYMIZE", "outputAction": "ANONYMIZE", "inputEnabled": true, "outputEnabled": true},
    {"type": "EMAIL", "action": "ANONYMIZE", "inputAction": "ANONYMIZE", "outputAction": "ANONYMIZE", "inputEnabled": true, "outputEnabled": true},
    {"type": "PHONE", "action": "ANONYMIZE", "inputAction": "ANONYMIZE", "outputAction": "ANONYMIZE", "inputEnabled": true, "outputEnabled": true},
    {"type": "ADDRESS", "action": "ANONYMIZE", "inputAction": "ANONYMIZE", "outputAction": "ANONYMIZE", "inputEnabled": true, "outputEnabled": true},
    {"type": "CREDIT_DEBIT_CARD_NUMBER", "action": "ANONYMIZE", "inputAction": "ANONYMIZE", "outputAction": "ANONYMIZE", "inputEnabled": true, "outputEnabled": true}
  ]
}
```

(see the [AWS documentation](https://docs.aws.amazon.com/bedrock/latest/userguide/guardrails-sensitive-filters.html)
for the full list of supported PII entity types), create the guardrail with:

```sh
aws bedrock create-guardrail --region eu-north-1 --name redacter-pii \
  --blocked-input-messaging "Blocked by guardrail" --blocked-outputs-messaging "Blocked by guardrail" \
  --sensitive-information-policy-config file://guardrail-pii.json
```

### Local rules redacter

The `local-rules` redacter runs entirely on your machine. It finds pattern-shaped personal information
with curated regular expressions and checksum validators (Luhn for payment cards, mod-97 for IBANs,
national checks for identifiers) and replaces each match with `[REDACTED]`. With `redacter scan` it reports
the matches instead, labelled with the name of the rule, see [Scan command](#scan-command).

On an image or a PDF, the file first goes through the OCR engine: every word it recognises is passed
to the redacter (and, with more than one `-d`, to the whole chain in turn) as if it were reading the
page as text, and every word the chain changes is blacked out on a rasterised copy of the page — the
output PDF or image carries no text layer for the original words to survive in.

Built-in rule groups, all enabled by default:

| Group | Contents |
|---|---|
| `email` | Email addresses |
| `phone` | International (`+`, `00`) and national phone numbers |
| `payment-card` | Payment card numbers (Luhn) |
| `iban` | IBANs (mod 97 and the national length) |
| `network` | IPv4, IPv6 and MAC addresses |
| `url` | Credentials embedded in URLs |
| `secrets` | AWS, GCP, GitHub, Slack and Stripe keys, JWTs, authorization headers, PEM and PGP private keys |
| `us-identifiers` | SSN and ITIN, passport numbers next to the word passport (in 9 languages), US driver's license numbers |
| `eu-identifiers` | European identifiers: the national personal numbers of Albania, Austria, Belgium, Bosnia and Herzegovina, Bulgaria, Croatia, Czechia, Denmark, Estonia, Finland, France, Germany (Steuer-ID), Greece (AMKA and AFM), Hungary (személyi szám and TAJ), Iceland, Ireland, Italy, Latvia, Lithuania, Luxembourg, Malta, Montenegro, the Netherlands, North Macedonia, Norway, Poland, Portugal, Romania, Serbia, Slovakia, Slovenia, Spain, Sweden, Switzerland, Turkey and the United Kingdom (NINO and driving licence), plus EU VAT numbers |
| `world-identifiers` | Canadian SIN, Australian TFN, Brazilian CPF, Indian Aadhaar, South African ID, Chinese resident ID |
| `postcodes` | UK postcodes, Irish Eircodes, Canadian postal codes |
| `birth-dates` | Dates of birth next to a birth keyword (`date of birth`, `Geburtsdatum`, `né le`, `fecha de nacimiento`, ...) in English, German, French, Spanish, Italian, Dutch, Portuguese, Polish and Swedish |

`eu-identifiers` covers the whole of Europe including the Nordics, Baltics, Balkans, Switzerland, Turkey
and the UK. Every rule with its check, whether it needs a context word, and an example is listed in
[docs/local-rules.md](docs/local-rules.md). Each identifier is verified with its published check digit
and, where the number embeds one, its birth date. Not covered, because the country publishes no format
or no check digit for its personal number: Cyprus, Liechtenstein, Monaco, Andorra, San Marino, Vatican
City and Kosovo; Ukraine, Moldova and Belarus are out of scope.

Identifiers whose check digit alone would accept too many ordinary numbers (Danish CPR, Portuguese NIF,
Greek AFM, Hungarian TAJ, Croatian OIB, Turkish TCKN, a bare Brazilian CPF, Canadian SIN, Australian TFN,
Indian Aadhaar) and shapes without a check (passports, driving licences, dates of birth, Albanian and
Maltese ids) are only redacted within 20 characters (30 for dates of birth) of a context word such as
`CPR`, `NIF`, `passport` or `date of birth`, in the local language. The keyword has to be in the same
text unit: a CSV cell is redacted on its own, so a column of bare CPFs under a `cpf` header is not found
while the written `NNN.NNN.NNN-NN` form is.

```sh
# Only emails and phone numbers
redacter cp -d local-rules --local-rules email,phone tmp/source/ tmp/redacted/

# Everything except the EU identifiers
redacter cp -d local-rules --local-rules-disable eu-identifiers tmp/source/ tmp/redacted/
```

You can add your own rules, similar to custom info types in GCP DLP, either inline or from a JSON file:

```sh
redacter cp -d local-rules --local-rule 'employee-id=\bEMP-[0-9]{6}\b' --local-rules-file rules.json ...
```

```json
{
  "rules": [
    { "name": "employee-id", "regex": "\\bEMP-[0-9]{6}\\b" },
    { "name": "project-codenames", "dictionary": ["Bluebird", "Kestrel"], "case_insensitive": true }
  ]
}
```

A rule has exactly one of `regex` or `dictionary`. Dictionary words match as whole words and are
case-insensitive unless `case_insensitive` is `false`. Rule names must be unique. User-defined rules
always run: they are not one of the switchable groups, so `--local-rules-disable custom` does not
suppress them; drop the `--local-rule` and `--local-rules-file` options instead.

The local rules redacter does not detect names, addresses or organisations, and it only matches
pattern-shaped values. Combine it with a cloud redacter to cover names and addresses, for example
`-d local-rules -d gcp-dlp` removes the pattern-shaped data locally before the remainder is sent to GCP
DLP. It works natively on text, html, json and csv files, and handles images and PDFs the same way the
AWS Comprehend redacter does: images through text extraction using OCR, blacking out the words the rules
matched, and PDF files by rendering them as images first. Both need the optional capabilities installed
(the OCR models, and Pdfium for PDFs); without them images and PDFs are skipped unless
`--allow-unsupported-copies` is set.

Matching on shape alone over-redacts in places. The known cases: a number of 13 to 19 digits starting
with 3 to 6 that passes the Luhn check is taken for a payment card, which also catches the rarer case of
a 13-digit EAN barcode starting with 3 to 6 whose own check digit happens to pass Luhn too
(`--local-rules-disable payment-card` is the lever for either); a national phone number written with
a leading `0` is any 9 to 12 digits in 2 to 5 groups, which also fits some reference numbers; any 6 to 9
digit number within 20 characters of the word `passport` is taken for a passport number; and national
identifiers without a keyword are accepted on their check digit and embedded date alone, so a bare
10-digit run of digits has about a 2% chance and an 11-digit run about 1.5% of passing one of the
checksummed rules. For a document that is mostly numbers, `--local-rules-disable eu-identifiers` (or
disabling the specific groups that don't apply) is the lever to pull.

### Local NER redacter

The `local-ner` redacter finds names of people (`per`), organisations (`org`) and locations (`loc`)
with a multilingual named-entity model running entirely on your machine, and replaces each of them
with `[REDACTED]`. It uses the `distilbert-base-multilingual-cased-ner-hrl` model, trained on ten
high-resource languages: Arabic, German, English, Spanish, French, Italian, Latvian, Dutch,
Portuguese and Chinese. Nothing leaves your computer; the only network access is
`redacter addons install local-ner`, which installs the model files described in [Addons](#addons).

Like `local-rules`, an image or a PDF first goes through the OCR engine: every recognised word is
passed to the model as if it were reading the page as text, and every word the chain redacts is
blacked out on a rasterised copy of the page.

```sh
# People, organisations and locations, the default
redacter cp -d local-ner tmp/source/ tmp/redacted/

# Only people, and redact more aggressively
redacter cp -d local-ner --local-ner-entities per --local-ner-min-score 0.3 tmp/source/ tmp/redacted/

# Pattern-shaped data by rules, names by the model, all offline
redacter cp -d local-rules -d local-ner tmp/source/ tmp/redacted/
```

Use a release binary (or `cargo run --release`) for `local-ner`: a debug build spends about 12
seconds per 512-token window of text, against about 95 ms in release. In release, loading the model
takes about 100 ms and it holds about 232 MiB of peak memory; the download in
[Addons](#addons) is about 131.6 MiB in total. A table costs one forward
pass per cell that holds a letter, so a CSV of many short rows is far slower than a text file of the
same size. These numbers are approximate and depend on your hardware and the amount of text.

`--local-ner-min-score` is the model confidence a word needs to be redacted, between 0 and 1
(default 0.5); lower values redact more and produce more false positives. The model does not detect
dates, emails, phone numbers or identifiers: combine it with `local-rules` for those. Its accuracy is
below the cloud DLP services, and the CPU time grows with the amount of text. It works natively on
text, html, json and csv files, and handles images and PDFs the way the AWS Comprehend redacter does:
through OCR and Pdfium, when those optional capabilities are installed.

A single word with nothing around it carries no context for the model, so a one-word text file may
not be tagged at all; a name in a table cell is scored together with its column header, which is why
a CSV or HTML table column of names is reliably redacted even though each cell is short. The `org`
label also over-redacts occasionally, tagging product and brand names alongside real organisations.

The model is [Xenova/distilbert-base-multilingual-cased-ner-hrl](https://huggingface.co/Xenova/distilbert-base-multilingual-cased-ner-hrl),
the ONNX export by Xenova of [Davlan/distilbert-base-multilingual-cased-ner-hrl](https://huggingface.co/Davlan/distilbert-base-multilingual-cased-ner-hrl)
by David Adelani, released under the [Academic Free License 3.0](https://opensource.org/license/afl-3-0-php).
The model is not bundled with the tool.

### Local GLiNER redacter

The `local-gliner` redacter runs a zero-shot span model entirely on your machine: instead of a fixed
label set, you give it the entity labels to look for as free text, and it scores every span of the
document against each one. It replaces each match with `[REDACTED]`, like the other local redacters.
Unlike `local-ner`, it reads context: it finds a lower-case name with no capitalisation to key off, a
medical condition described in a sentence, a date of birth with no `date of birth` keyword next to it,
and handles and usernames written as free text, none of which `local-rules` or `local-ner` can find on
their own. It uses [`urchade/gliner_multi_pii-v1`](https://huggingface.co/urchade/gliner_multi_pii-v1)
(the [onnx-community](https://huggingface.co/onnx-community/gliner_multi_pii-v1) ONNX export),
trained on six languages: English, French, German, Spanish, Italian and Portuguese. Nothing leaves your
computer; the only network access is `redacter addons install local-gliner`, which installs the model files
described in [Addons](#addons).

Like `local-rules` and `local-ner`, an image or a PDF first goes through the OCR engine: every recognised
word is passed to the model as if it were reading the page as text, and every word the chain redacts is
blacked out on a rasterised copy of the page.

```sh
# The default PII label list
redacter cp -d local-gliner tmp/source/ tmp/redacted/

# Your own labels, and redact more aggressively
redacter cp -d local-gliner --local-gliner-labels "person,email,medical condition" --local-gliner-min-score 0.3 tmp/source/ tmp/redacted/

# Pattern-shaped data by rules, everything else by the context-aware model, all offline
redacter cp -d local-rules -d local-gliner tmp/source/ tmp/redacted/
```

`--local-gliner-labels` is a comma-separated list of free-text labels, understood by the model rather
than looked up in a fixed taxonomy; the default is a PII-only list: person, address, email, phone
number, date of birth, passport number, national id number, credit card number, iban, ip address,
username, medical condition, postal code, license plate number. `organization` is deliberately not in
the default list: measured against this model, it tags team and desk names such as "Marketing team" and
"The Support Desk" as organisations, which is over-redaction for a label most callers do not want on by
default; add it with `--local-gliner-labels` when you do. `--local-gliner-min-score` is the model
confidence a span needs to be redacted, between 0 and 1 (default 0.5); lower values redact more. Scores
are well separated in practice, so lowering the threshold rarely changes the result.

Use a release binary (or `cargo run --release`) for `local-gliner`: like `local-ner`, a debug build is
far slower than release. In release, on an Intel i7-10700K (16 threads), loading the model takes about
0.6 s and it holds about 1.5 GB of peak memory while redacting a small text corpus; the download in
[Addons](#addons) is about 1.17 GB, almost all of it the fp32 model weights
(the int8 export cannot be used: its span head has an operator this tool's ONNX runtime does not
support). On the benchmark corpus under `test-fixtures/bench-dlp/` this measured 834 ms per file and
2.8 s over the whole 8-file corpus. The per-file figure includes process start and the ~0.6 s model
load, since the benchmark harness invokes the binary once per file rather than scoring in one
long-lived process; the forward pass itself takes roughly 100-200 ms per 384-token window. A document
scores each label in batches of at most 25, so more labels cost more passes and, as with
`organization` above, more false positives.

A date that merely looks like a date of birth may still be redacted regardless of context, since the
model has learned the shape as well as the wording -- on the benchmark corpus this catches the invoice
date `14 March 1985` in `dates-en.txt`, which carries no birth-related wording at all. A single word with
nothing around it can still be missed the way it can with `local-ner`, since the model also relies on
context to score a span highly.

The model is Apache-2.0, [`urchade/gliner_multi_pii-v1`](https://huggingface.co/urchade/gliner_multi_pii-v1)
by Urchade Zaratiana, trained on the Apache-2.0
[`urchade/synthetic-pii-ner-mistral-v1`](https://huggingface.co/datasets/urchade/synthetic-pii-ner-mistral-v1)
dataset; its backbone, `microsoft/mdeberta-v3-base`, is MIT licensed. The model is not bundled with the
tool.

### Local or cloud: quality versus performance

The three local redacters run entirely on your machine, so nothing leaves it, nothing is billed and nothing needs
credentials. The price is detection quality: curated rules only find what has a shape (emails, phones, cards,
IBANs, keys, national ids with checksums); a small named-entity model only finds persons, organisations and
locations in the ten languages it was trained on; a zero-shot span model (`local-gliner`) reads context well
enough to close most of that gap, at several times the latency and over a gigabyte of memory. A cloud DLP
service knows hundreds of info types, and an LLM reasons over the whole document, so both can still find more
in the hardest cases, at the cost of latency, money, and sending the document out.

Pros of local redaction:

- Privacy by construction: works air-gapped, no account, no per-request cost, no rate limits.
- Speed: rules run in microseconds; the NER model needs about 100 ms per 512 tokens on a desktop CPU;
  GLiNER is slower, under a second per file, but still local and free.
- Determinism: the same input always gives the same output, which makes results easy to test and audit.
- A useful pre-pass: `-d local-rules -d local-ner -d gcp-dlp` (or `-d local-gliner` in place of `local-ner`)
  removes the obvious PII before anything is sent.

Cons of local redaction:

- Narrower coverage without `local-gliner`: `local-rules` alone has no free-form addresses or document-level
  reasoning, and only redacts dates of birth, passport and licence numbers next to a context keyword.
- Model quality: the 66M-parameter NER model misses names it has never seen, needs context to tag a single
  word, and reports organisations for product names; GLiNER can mistake a plain date for a date of birth by
  shape alone, and more labels bring more of that kind of false positive.
- Resource cost: `local-gliner`'s model is a ~1.17 GB download and needs about 1.5 GB of memory while running,
  against `local-ner`'s 132 MB and 232 MB.
- Locale-bound rules: national formats differ; expect to enable, disable or add rules for your documents.

Measured on the corpus under `test-fixtures/bench-dlp/` (eight fixture documents including a contextual note
written so its PII needs context rather than shape to find, about 3.7 KB of text; local rows measured at
commit `e9b325d`, Intel i7-10700K, one warm-up then the median of three runs; per-file figures include
process start and model load, since the harness invokes the binary once per file). The cloud rows are kept
from an earlier run on a 7-file corpus (commit `3c3491e`, before the contextual file was added, 41 PII / 7
entities / 25 keep) and were not re-run against the current corpus, so they are not directly comparable to
the PII/kept counts below:

| Redacter | Whole corpus | Per file (median) | PII removed | Cities and organisations removed | Non-PII kept |
|---|---|---|---|---|---|
| `local-rules` | 52 ms | 49 ms | 28 / 48 | 0 / 8 | 31 / 31 |
| `local-ner` | 441 ms | 152 ms | 17 / 48 | 8 / 8 | 27 / 31 |
| `local-rules` + `local-ner` | 466 ms | 205 ms | 43 / 48 | 8 / 8 | 27 / 31 |
| `local-gliner` | 2.8 s | 834 ms | 48 / 48 | 1 / 8 | 28 / 31 |
| `local-rules` + `local-gliner` | 2.7 s | 878 ms | 48 / 48 | 1 / 8 | 27 / 31 |
| `gcp-dlp` (7-file corpus) | 710 ms | 266 ms | 40 / 41 | 4 / 7 | 25 / 25 |
| `gcp-vertex-ai` (Gemini, 7-file corpus) | 68.9 s | 6.0 s | 41 / 41 | 1 / 7 | 24 / 25 |

`local-rules` still leaves behind the person names and free-form street addresses that have no shape to
match; `local-rules+local-ner` closes most of that gap but, with no notion of context, still misses the
contextual file's lower-case name, medical condition, keyword-less date of birth and two handles (43/48).
`local-gliner` reaches 48/48 with its default 14-label PII list on its own -- every PII string in the
corpus, including all of the contextual file's and the digit-only phone numbers in `customers.csv` --
and chained after `local-rules` also stays at 48/48, since `local-rules` already covers the same
structured PII on its own. Since `organization` is off by default, `local-gliner` redacts only the one
`entities` string embedded in a free-form address (1/8) rather than `local-ner`'s 8/8; entity coverage is
available via `--local-gliner-labels` but costs the false positives that come with it. The keep losses
beyond `local-rules`' clean 31/31 are the known over-redaction cases: `local-ner` and the `local-ner`
chain tag "Apple" and "The Support Desk" as organisations; `local-gliner` and its chain tag the word "email"
in a sentence about contact preferences as an email address, and mistake the invoice date
`14 March 1985` in `dates-en.txt` for a date of birth by shape alone, the same false positive noted in the
[Local GLiNER redacter](#local-gliner-redacter) section.
Run `cargo test --release --test bench_dlp -- --ignored --nocapture` (see
`test-fixtures/bench-dlp/README.md`) to reproduce the table on your own machine and documents.

In short: use the local redacters when the data must not leave the machine or when you need a fast, cheap
first pass; use a cloud provider, ideally after the local pass, when coverage matters more than latency and cost.

## Multiple redacters

You can specify multiple redacters using `--redact` option multiple times.
The tool will apply redaction in the order of the redacters specified.
When multiple redacters are specified, the tool first tries to redact the file using the redacters that support input
files natively.
Only if such redacters are not available, the tool will try to redact the file using the redacters using conversions.

`scan` does not chain the redacters: each one reads the original file and reports its own findings.

## PDF redaction

PDF redaction is supported by rendering PDF files as images and redacting them.
To render and convert PDF files the tool uses the external library `Pdfium` (the C++ PDF library used by the Google
Chromium project). `redacter addons install pdf` installs the build the tool is compiled against:

- The library is stored with the other addons, in `<addons dir>/pdfium-7881/`, and is checked against its pinned
  SHA-256 every time it is loaded. A copy that does not match turns PDF rendering off, reported as
  `PDF rendering: no (redacter addons install pdf)`; no other Pdfium is tried in its place.
- The addon is available for Linux (glibc) on x86_64 and aarch64, macOS on x86_64 and aarch64, and Windows on x86,
  x86_64 and aarch64. Other platforms, such as FreeBSD and the static musl builds, need Pdfium installed by hand.

Without the addon the tool looks for a Pdfium installed by hand. It tries the `lib` directory beside the directory
of the tool (for example `/usr/local/lib` for `/usr/local/bin/redacter`) and the directory of the tool itself, then:

- on Linux and other Unix systems (not macOS), the system loader, which finds `libpdfium.so` in the system
  library directories and in `LD_LIBRARY_PATH`; an empty entry in `LD_LIBRARY_PATH` means the current directory;
- on macOS, `/opt/homebrew/lib`, `/usr/local/lib` and `/usr/lib`;
- on Windows, nothing else.

The current directory is not searched otherwise.

Manual installation:

- Download the Pdfium build of
  [release chromium/7881](https://github.com/bblanchon/pdfium-binaries/releases/tag/chromium%2F7881) for your system
  from the bblanchon/pdfium-binaries project. Other builds may not match the library the tool is compiled against.
- Extract the archive and copy the library file (`libpdfium.so`, `libpdfium.dylib` or `pdfium.dll`) to the directory
  of the tool.

If the library is detected correctly it is reported in the tool output as:
> PDF rendering: yes

## Addons

The OCR engine, PDF rendering and the `local-ner` and `local-gliner` redacters need files that are not
bundled with the tool. `redacter addons install` installs them and is the only command that downloads anything:
`cp` uses what is installed.

- A missing OCR or Pdfium addon turns that capability off for the run, reported for example
  as `OCR: no (redacter addons install ocr)`.
- A missing `local-ner` or `local-gliner` addon stops the run with an error naming the command that installs it:
  a file is never copied unredacted because the redacter asked for could not start.

```sh
# OCR and PDF rendering
redacter addons install ocr,pdf
# The local-ner model, into a directory of your choice
redacter --addons-dir /opt/redacter/addons addons install local-ner
# What this build offers, and what is installed
redacter addons ls
```

`redacter addons ls` only looks: it prints one line per addon of this build with its state (`installed`,
`not installed`, `damaged`, `not available for this platform` or `unreadable`, which means the addon's files
could not be read), its size and where it was found. It downloads
nothing. For `pdf` it checks the addons directory only, so a Pdfium installed by hand, which `cp` uses, is listed
as `not installed`.

The files are hosted as assets of GitHub releases in this repository, copied unmodified from the sources listed
below:

| Addon | Files | Size | Source | Licence |
|---|---|---|---|---|
| `ocr`, in `ocrs/` | `text-detection.rten`, `text-recognition.rten` | 11.7 MiB | [release `models-ocrs-1`](https://github.com/abdolence/redacter-rs/releases/tag/models-ocrs-1), from the [ocrs](https://github.com/robertknight/ocrs) project | MIT OR Apache-2.0 (ocrs); weights trained on open, liberally licensed datasets |
| `pdf`, in `pdfium-7881/` | `libpdfium.so`, `libpdfium.dylib` or `pdfium.dll`, depending on the platform | 6.3 to 7.5 MiB | [release `models-pdfium-7881`](https://github.com/abdolence/redacter-rs/releases/tag/models-pdfium-7881), from [bblanchon/pdfium-binaries](https://github.com/bblanchon/pdfium-binaries) `chromium/7881` (Pdfium 151.0.7881.0) | Pdfium and the libraries it bundles under their own licences, attached to the release |
| `local-ner`, in `distilbert-base-multilingual-cased-ner-hrl/` | `model_uint8.onnx`, `tokenizer.json`, `config.json` | 131.6 MiB | [release `models-ner-hrl-1`](https://github.com/abdolence/redacter-rs/releases/tag/models-ner-hrl-1), from [Xenova/distilbert-base-multilingual-cased-ner-hrl](https://huggingface.co/Xenova/distilbert-base-multilingual-cased-ner-hrl) (revision `c2a4dbf`) | AFL-3.0 |
| `local-gliner`, in `gliner_multi_pii-v1/` | `model.onnx`, `tokenizer.json`, `gliner_config.json` | 1.1 GiB | [release `models-gliner-multi-pii-1`](https://github.com/abdolence/redacter-rs/releases/tag/models-gliner-multi-pii-1), from [onnx-community/gliner_multi_pii-v1](https://huggingface.co/onnx-community/gliner_multi_pii-v1) (revision `2e0397a7`) | Apache-2.0 (model and training data); backbone `microsoft/mdeberta-v3-base` MIT |

Every downloaded file is verified against a SHA-256 digest pinned in the tool before it takes its final name.
The addons are stored under `~/.cache/redacter/addons/` on Linux, `~/Library/Caches/redacter/addons/` on macOS
and `%LOCALAPPDATA%\redacter\addons\` on Windows, one directory per addon:

- `addons install` checks an installed addon against its digests and skips it when it matches, and replaces a
  damaged one;
- an addon directory that holds only some of the addon's files is reported as damaged, and the manual copies below
  are not used in its place;
- a run checks the Pdfium library against its digest every time it loads it, and a model against its pinned size;
- `--addons-dir <DIR>` (or the `REDACTER_ADDONS_DIR` environment variable) moves that directory, for
  `addons install`, `addons ls` and `cp` alike;
- downloads honour the `HTTPS_PROXY` environment variable.

An interrupted install can leave `*.part` files in the addon's directory; they are safe to delete while no install is running.

For air-gapped machines, download the files listed above on another computer and copy them into the addon's
directory under the names in the table, for example `~/.cache/redacter/addons/ocrs/text-detection.rten`. This works
for Pdfium too, which is checked against its digest. For the models the tool also looks in
`addons/<addon directory>/` next to its own executable. Breaking change: earlier versions used `models/<addon directory>/`;
move it to `addons/` or run `redacter addons install` again. The OCR models are still found in `~/.cache/ocrs` and
`../share/ocrs` as in earlier versions; these copies are used as they are, checked against nothing.

`--download-models` and its download prompt are removed: install the addons with `addons install` instead.
`--models-dir` and `REDACTER_MODELS_DIR` are replaced by `--addons-dir` and `REDACTER_ADDONS_DIR`, and the default
directory is `addons` instead of `models`: move the old `models` directory to `addons` or run
`redacter addons install` again. For the same reason the `models` directory next to the executable is now named
`addons`.

## OCR

The tool supports OCR for images and PDF files using [ocrs engine](https://github.com/robertknight/ocrs).
Install the OCR models with `redacter addons install ocr`, see [Addons](#addons); `~/.cache/ocrs` from earlier
versions keeps working.

## Examples:

```sh
# Copy and redact a file from local filesystem to GCS
redacter cp -d gcp-dlp --gcp-project-id <your-gcp-project-with-dlp> sensitive.png gs://my-bucket-name/test/test.png  
```

The tool supports recursive copy of multiple files from directory:

```sh
redacter cp s3://my-bucket-name/sensitive-files/ tmp/
```

Zip archives are supported too:

```sh
redacter cp gs://my-bucket-name/sensitive-files/ zip://tmp/sensitive-files.zip
```

Filter files by name:

```sh
redacter cp -f "*.jpg" ...
```

and/or by size:

```sh
redacter cp -m 1024 ...
```

MS Presidio redacter:

```sh
redacter cp -d ms-presidio --ms-presidio-text-analyze-url http://localhost:5002/analyze --ms-presidio-image-redact-url http://localhost:5003/redact ...
```

Vertex AI redacter:

```sh
redacter cp -d gcp-vertex-ai --gcp-project-id my-little-project tmp/source/ tmp/redacted/
```

AWS Bedrock redacter:

```sh
redacter cp -d aws-bedrock --aws-region us-east-1 tmp/source/ tmp/redacted/
```

AWS Bedrock Guardrails redacter:

```sh
redacter cp -d aws-bedrock-guardrails --aws-region eu-north-1 --aws-bedrock-guardrail-id <id> tmp/source/ tmp/redacted/
```

Local rules redacter, entirely offline:

```sh
redacter cp -d local-rules tmp/source/ tmp/redacted/
```

Override media types based on filenames:

```sh
redacter cp --mime-override "text/plain=*.bin" ...
```

Redact an image from clipboard:

```sh
redacter cp clipboard:// tmp/image/ ...
```

## Scan command

The `scan` command runs the selected redacters in detect-only mode. It reads every file of the source and reports each
piece of sensitive data it detects and where it is, called a finding. Nothing is copied or changed, so there is no
destination.

```
redacter scan [OPTIONS] --redact <REDACT> <SOURCE>
```

The source is anything `cp` reads: a local file or directory, GCS, S3 or a zip archive. The options that select and
configure the redacters are the ones of `cp`. Specific to `scan`, or worth knowing about:

- `-d`, `--redact`: the redacters to run, at least one; repeat the option for more. Every redacter reads the same
  original file on its own, with no chain and no merge between them, so a value found by two redacters is reported
  twice, once for each;
- `-m`, `-n`, `-f`, `--mime-override`, `--sampling-size`, `--csv-headers-disable`, `--csv-delimiter`: as in `cp`;
- `--report-file <FILE>`: write the report to a file, besides the terminal output;
- `--report-format <json|text>`: the format of that file, `json` by default; requires `--report-file`;
- `--show-values`: show the matched values instead of masking them;
- `--min-score <0..1>`: drop the findings that have a score below this value;
- `--no-fail`: exit with 0 when there are findings.

```sh
redacter scan tmp/source/ -d local-rules
```

```
Redacter v0.20.0 (https://github.com/abdolence/redacter-rs)
Scanning tmp/source/ with local-rules. PDF rendering: no · OCR: no

customers.csv:1:2  email  local-rules  -  j***m
customers.csv:2:2  email  local-rules  -  j***g
notes.txt:1:10  email  local-rules  -  j***m
notes.txt:2:8  phone-international  local-rules  -  +***8
notes.txt:3:7  payment-card  local-rules  -  4***1

3 files: 2 with findings, 1 clean, 0 skipped, 0 unsupported. 5 findings: email 3, payment-card 1, phone-international 1.
```

A finding is one line: the location, the label, the redacter that found it, the score and the value. A file without
findings has no line, as with `grep`. The location depends on the content:

- text: `path:line:column`, both counted from 1, the column in characters;
- a cell of a CSV table: `path:row:column`, both counted from 1, the header row not counted. A redacter that reads
  text but not tables, such as Microsoft Presidio, gets the CSV file as plain text and reports `path:line:column` of
  the file instead, where the header is line 1;
- an image: `path:[x1,y1,x2,y2]`, a box in pixels of the image;
- a page of a PDF file: `path:page:[x1,y1,x2,y2]`, the page counted from 1 and the box in pixels of the rendered page.

On an image or a PDF page the box covers the words OCR recognised for the matched text. A redacter that reads
images itself reports the box it found.

A file that is not scanned or failed gets a line with the reason instead, such as
`customer-form.pdf  blocked: pdf renderer unavailable` or `blob.bin  unsupported: type not supported`.

### Masking

Values are masked by default, so a report can be shared without spreading what it found: the first and the last
character with `***` between them for a value of 6 or more characters, and `***` for a shorter one. There is no
hash of the value, since a hash of a phone number or an ID can be reversed by trying every candidate.

`--show-values` shows the values as found, in the terminal output and in the report file.
Be aware that a report written with `--show-values` contains the sensitive data itself.

### Scores

A score is the confidence of the redacter that the value is what the label says, between 0 and 1. The local-ner,
local-gliner, AWS Comprehend, Microsoft Presidio and GCP DLP redacters report one. The local-rules redacter, the LLM
redacters and AWS Bedrock Guardrails do not: their findings show `-` in the terminal and `null` in the JSON report.

`--min-score` drops the findings below the given score. A finding without a score is kept, so the option never hides
what a rule-based or an LLM redacter found.

```sh
redacter scan test-fixtures/documents/customer-note.txt -d local-rules -d local-ner --min-score 0.9
```

```
customer-note.txt:4:8  email  local-rules  -  j***m
customer-note.txt:5:8  phone-international  local-rules  -  +***7
customer-note.txt:6:16  birth-date  local-rules  -  1***5
customer-note.txt:7:36  uk-postcode  local-rules  -  N***E
customer-note.txt:10:14  phone-international  local-rules  -  +***7
customer-note.txt:10:57  email  local-rules  -  j***m
customer-note.txt:11:34  payment-card  local-rules  -  4***1
customer-note.txt:3:11  per  local-ner  1.00  J***h
customer-note.txt:7:15  loc  local-ner  0.96  B***t
customer-note.txt:7:29  loc  local-ner  1.00  L***n
customer-note.txt:9:1  per  local-ner  0.99  ***
```

Without `--min-score` the same scan also reports `customer-note.txt:7:10  loc  local-ner  0.59  ***`. Every
local-rules finding is kept in both runs.

GCP DLP does not give a number. It rates each finding with a likelihood, one of five levels of how likely the
value is to be of the reported info type, and `scan` turns it into a fixed score:

| DLP likelihood | Score |
|---|---|
| `VERY_UNLIKELY` | 0.1 |
| `UNLIKELY` | 0.3 |
| `POSSIBLE` | 0.5 |
| `LIKELY` | 0.7 |
| `VERY_LIKELY` | 0.9 |

A finding with an unspecified likelihood has no score.

`--local-ner-min-score` and `--local-gliner-min-score` work as in `cp`: a local-ner or local-gliner finding is only
reported when its score reaches them (0.5 by default).

### Exit codes

- `0`: nothing was found, or `--no-fail` is set;
- `2`: at least one finding was reported;
- `1`: an error. A file that could not be read or scanned is reported with the status `failed`, and a file that needs a
  converter the run lacks, such as OCR or PDF rendering, with the status `blocked`. The other files are still scanned,
  and the command exits with 1 even if there are findings, since the scan is incomplete. An invalid command line exits
  with 1 as well, for every command, so that 2 always means findings.

Skipped and unsupported files do not change the exit code. `--no-fail` does not change an error exit code.

A gate in a CI pipeline is the command itself, as the job fails on a non-zero exit code:

```sh
# Exits with 2 and fails the job when anything is found
redacter scan ./docs -d local-rules --report-file scan-report.json

# Writes the same report and always exits with 0, for a first run over an existing tree
redacter scan ./docs -d local-rules --report-file scan-report.json --no-fail
```

### JSON report

`--report-file <FILE>` writes the report to a file in JSON by default. The terminal always shows the usual output,
whatever the report options are; there is no mode that prints the JSON to stdout.

```json
{
  "files_scanned": 1,
  "files_with_findings": 1,
  "files_skipped": 0,
  "files_unsupported": 0,
  "files_blocked": 0,
  "files_failed": 0,
  "findings": 2,
  "findings_by_label": {
    "email": 1,
    "payment-card": 1
  },
  "files": [
    {
      "path": "notes.txt",
      "mime_type": "text/plain",
      "status": "scanned",
      "reason": null,
      "findings": [
        {
          "redacter": "local-rules",
          "label": "email",
          "score": null,
          "location": {
            "kind": "text",
            "start": 9,
            "end": 29,
            "line": 1,
            "column": 10
          },
          "value": "j***m"
        },
        {
          "redacter": "local-rules",
          "label": "payment-card",
          "score": null,
          "location": {
            "kind": "text",
            "start": 36,
            "end": 55,
            "line": 2,
            "column": 7
          },
          "value": "4***1"
        }
      ]
    }
  ]
}
```

At the top level:

- `files_scanned`: files read by the redacters, with or without findings;
- `files_with_findings`, `findings`: how many files and how many findings the report has;
- `files_skipped`: files left out by `--max-size-limit` or `--filename-filter`;
- `files_unsupported`: files that no selected redacter reads, including image formats that OCR cannot read, such as
  SVG;
- `files_blocked`: files that need a converter the run lacks, such as OCR or PDF rendering;
- `files_failed`: files that could not be read or scanned;
- `findings_by_label`: the number of findings for each label;
- `files`: one entry for every file that was scanned, unsupported or failed. Files that the filters leave out of a
  directory listing are only counted in `files_skipped`.

Each entry of `files` has the `path` relative to the source, the `mime_type`, the `status` and the `findings` of the
file. The `status` is `scanned`, `unsupported`, `blocked` (a converter is missing, counted in `files_blocked`),
`skipped` or `failed`. The `reason` is `null` for a scanned file and says why for the others.

Each finding has the `redacter` that found it, the `label`, the `score` (`null` when the redacter gives none), the
`location` and the `value`, masked unless `--show-values` is set. The `location` has a `kind`:

- `text`: `start` and `end` are byte offsets in the text of the file, with the `line` and `column` of the start;
- `table_cell`: the `row` and `column` of the cell, both counted from 1, and the byte offsets `start` and `end` in the
  cell, for example `{"kind":"table_cell","row":1,"column":2,"start":0,"end":20}`;
- `image`: the box `x1`, `y1`, `x2`, `y2` in pixels, and the `page` of a PDF file counted from 1 (`null` for an image),
  for example `{"kind":"image","page":null,"x1":308.0,"y1":192.0,"x2":621.0,"y2":219.0}`.

Fields are only ever added to the report, never renamed or removed.

### Text report

`--report-format text` writes what the terminal shows for the files and the summary, without colours.

```sh
redacter scan tmp/source/ -d local-rules --report-file scan-report.txt --report-format text
```

```
notes.txt:1:10  email  local-rules  -  j***m
notes.txt:2:7  payment-card  local-rules  -  4***1

1 file: 1 with findings, 0 clean, 0 skipped, 0 unsupported. 2 findings: email 1, payment-card 1.
```

### Detection per redacter

Each redacter detects in its own way and names the data with its own labels. The labels are not translated between
redacters: an email address is `email` for local-rules, `EMAIL_ADDRESS` for GCP DLP and Microsoft Presidio and `EMAIL`
for AWS Comprehend.

| Redacter | Label | Score | Images and PDF pages |
|---|---|---|---|
| `local-rules` | The rule name, such as `email` or `payment-card`, see [docs/local-rules.md](docs/local-rules.md) | None | OCR |
| `local-ner` | `per`, `org` or `loc` | Model confidence | OCR |
| `local-gliner` | The label from `--local-gliner-labels`, such as `person` | Model confidence | OCR |
| `gcp-dlp` | The info type, such as `EMAIL_ADDRESS` | From the likelihood, see [Scores](#scores) | Read by DLP, which reports the boxes |
| `aws-comprehend` | The PII entity type, such as `EMAIL` | Model confidence | OCR |
| `ms-presidio` | The entity type, such as `EMAIL_ADDRESS` | Presidio score | OCR |
| `aws-bedrock-guardrails` | The PII type of the guardrail, or the name of its regex | None | OCR |
| `gemini-llm`, `open-ai-llm`, `gcp-vertex-ai`, `aws-bedrock` | The type the model names for each value | None | The coordinate pass of redaction |

- Images and PDF pages go through OCR for the redacters that cannot read images, and need the OCR models and, for PDF
  files, Pdfium, as for redaction. Without them the file is reported as `blocked`.
- A CSV file is read as a table by the redacters that support tables, and the findings have a `table_cell` location.
  A redacter that reads text but not tables, such as Microsoft Presidio, gets the file as plain text.
- Microsoft Presidio needs `--ms-presidio-text-analyze-url` to detect. The image redaction endpoint reports no
  locations, so images are read with OCR and the text is analysed. With only the image URL set, the files are reported
  as unsupported.
- The LLM redacters ask the model for the sensitive values in the text with a type for each, and report every
  occurrence of each value, ignoring case, since the model may not copy a value letter for letter. An occurrence is
  reported wherever it is, so a short value is also found inside longer words: `John` is reported inside `Johnson`,
  which covers inflected words and scripts written without spaces. An occurrence inside a longer reported one is
  dropped. Letters are compared one at a time, so `ß` and `SS` differ, while Greek `ς` and `Σ` match.
  For an image they use the same coordinate pass as redaction, and one more text call per image gets the labels.
  `--llm-image-mode` has no effect on `scan`.
- An answer that the provider filtered, cut short or left empty fails the file, in `scan` and in `cp`; it is never
  reported as a clean file or written out as redacted text. `gcp-vertex-ai` and `gemini-llm` ask once more after a
  `SAFETY` or `OTHER` stop or an empty answer. The safety threshold of `gcp-vertex-ai` is set by
  `--gcp-vertex-ai-block-none-harmful`.
- AWS Bedrock Guardrails reports the matched value and its type but no position, so the findings are located by
  searching the text for the value in the same way.

## List (LS) command

For convenience, the tool also supports listing files in the source directory so you can see what files will be copied:

```
Usage: redacter ls [OPTIONS] <SOURCE>

Arguments:
  <SOURCE>  Source directory or file such as /tmp, /tmp/file.txt or gs://bucket/file.txt and others supported providers

Options:
  -m, --max-size-limit <MAX_SIZE_LIMIT>    Maximum size of files to copy in bytes
  -f, --filename-filter <FILENAME_FILTER>  Filter by name using glob patterns such as *.txt
  -h, --help                               Print help
```

Example: list files in the GCS bucket:

```sh
redacter ls gs://my-little-bucket/my-big-files/
```

## Security considerations

- Your file contents are sent to the DLP API for redaction or scanning. Make sure you trust the DLP API provider.
- A scan report masks the values it found unless `--show-values` is set. A report written with `--show-values` contains
  the sensitive data itself, so store it as you would the files it came from.
- The local redacters (`local-rules`, `local-ner`, `local-gliner`) send nothing anywhere. Their model
  files are fetched only on request, over HTTPS, and checked against SHA-256 digests pinned in the tool.
- The accuracy of redaction depends on the DLP model, so don't rely on it as the only security measure.
- The tool was mostly designed to redact files internally. Not recommended to use it in public environments without
  apropriate security measures and manual review.
- Integrity of the files is not guaranteed due to DLP implementation specifics. Some formats such as
  HTML/XML/JSON may be corrupted after redaction since they treated as text.
- Use it at your own risk. The author is not responsible for any data loss or security breaches.

## Recommended DLP providers

Google Cloud Platform DLP is recommended for accurate and customizable redaction, since it is one of the most advanced
DLP on the market.
Image redaction in LLM models right now results in not very accurate redaction, however text redaction is quite good.

For the most protection, you can use multiple DLP providers in the order of the most accurate to the least accurate.

## Licence

Apache Software License (ASL)

## Author

Abdulla Abdurakhmanov
