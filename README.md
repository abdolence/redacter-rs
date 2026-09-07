[![Cargo](https://img.shields.io/crates/v/redacter.svg)](https://crates.io/crates/redacter)
![tests and formatting](https://github.com/abdolence/redacter-rs/workflows/tests%20&amp;%20formatting/badge.svg)
![security audit](https://github.com/abdolence/redacter-rs/workflows/security%20audit/badge.svg)

# Redacter

Copy & Redact cli tool to securely copy and redact files removing Personal Identifiable Information (PII)
across various sources and destinations and utilizing Data Loss Prevention (DLP) capabilities.

The tool ships two redacters that run entirely on your machine, a rule-based one with checksum validation and a
small multilingual named-entity model, and can also delegate to cloud DLP services and LLMs such as Google Cloud
Platform's DLP API. Local redaction keeps the data on the host and costs nothing per file; the cloud providers
find more. See [Local or cloud](#local-or-cloud-quality-versus-performance) for the measured trade-off.

![redacter-demo](media/redacter-demo.gif)

## Features

* **Copy & Redact:**  copy files while applying DLP redaction to protect sensitive information.
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

- If you are planning to use PDF redaction, please follow additional steps in
the [PDF redaction](#pdf-redaction)
- OCR (for the DLP providers that can't read images) and the `local-ner` redacter need model files that
  are downloaded on request or installed by hand, see [Models and downloads](#models-and-downloads).

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
      --download-models <DOWNLOAD_MODELS>
          Whether missing model files (OCR, local-ner) may be downloaded: 'ask' prompts on the terminal and behaves as 'no' when stdin or stderr is not a terminal, 'yes' downloads without asking, 'no' never downloads. Default is 'ask'
          
          [default: ask]
          [possible values: ask, yes, no]

  -m, --max-size-limit <MAX_SIZE_LIMIT>
          Maximum size of files to copy in bytes

      --models-dir <MODELS_DIR>
          Directory holding downloaded and manually placed models, one subdirectory per model. Overrides the REDACTER_MODELS_DIR environment variable. Default is the user cache directory, for example ~/.cache/redacter/models

  -n, --max-files-limit <MAX_FILES_LIMIT>
          Maximum number of files to copy. Sort order is not guaranteed and depends on the provider

  -f, --filename-filter <FILENAME_FILTER>
          Filter by name using glob patterns such as *.txt

  -d, --redact <REDACT>
          List of redacters to use
          
          [possible values: gcp-dlp, aws-comprehend, ms-presidio, gemini-llm, open-ai-llm, gcp-vertex-ai, aws-bedrock, aws-bedrock-guardrails, local-rules, local-ner]

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

      --mime-override <MIME_OVERRIDE>
          Override media type detection using glob patterns such as 'text/plain=*.md'

      --save-json-results <SAVE_JSON_RESULTS>
          Save redacted results in JSON format to the specified file

  -h, --help
          Print help (see a summary with '-h')
```

DLP is optional and should be enabled with `--redact` (`-d`) option.
Without DLP enabled, the tool will copy all files without redaction.
With DLP enabled, the tool will redact files based on the DLP model and skip unsupported files.

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
national checks for identifiers) and replaces each match with `[REDACTED]`.

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
with 3 to 6 that passes the Luhn check is taken for a payment card; a national phone number written with
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
Portuguese and Chinese. Nothing leaves your computer; the only network access is the one-time,
opt-in download of the model files described in [Models and downloads](#models-and-downloads).

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
[Models and downloads](#models-and-downloads) is about 131.6 MiB in total. A table costs one forward
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

### Local or cloud: quality versus performance

The two local redacters run entirely on your machine, so nothing leaves it, nothing is billed and nothing needs
credentials. The price is detection quality: curated rules only find what has a shape (emails, phones, cards,
IBANs, keys, national ids with checksums), and a small named-entity model only finds persons, organisations and
locations in the ten languages it was trained on. A cloud DLP service knows hundreds of info types, and an LLM
understands context, so both find more, at the cost of latency, money, and sending the document out.

Pros of local redaction:

- Privacy by construction: works air-gapped, no account, no per-request cost, no rate limits.
- Speed: rules run in microseconds; the NER model needs about 100 ms per 512 tokens on a desktop CPU.
- Determinism: the same input always gives the same output, which makes results easy to test and audit.
- A useful pre-pass: `-d local-rules -d local-ner -d gcp-dlp` removes the obvious PII before anything is sent.

Cons of local redaction:

- Narrower coverage: no free-form addresses and no document-level reasoning; dates of birth, passport and
  licence numbers only next to a context keyword.
- Model quality: a 66M-parameter model misses names it has never seen, needs context to tag a single word,
  and reports organisations for product names.
- Locale-bound rules: national formats differ; expect to enable, disable or add rules for your documents.

Measured on the small corpus under `test-fixtures/bench-dlp/` (four fixture documents plus a multilingual sample,
about 4 KB of text; local rows at commit `540fbc5`, cloud rows from the run at commit `3c3491e`, Intel i7-10700K,
one warm-up then the median of three runs; the cloud numbers include network time from Europe):

| Redacter | Whole corpus | Per file (median) | PII removed | Cities and organisations removed | Non-PII kept |
|---|---|---|---|---|---|
| `local-rules` | 38 ms | 35 ms | 28 / 41 | 0 / 7 | 25 / 25 |
| `local-ner` | 370 ms | 144 ms | 15 / 41 | 7 / 7 | 23 / 25 |
| `local-rules` + `local-ner` | 431 ms | 174 ms | 41 / 41 | 7 / 7 | 23 / 25 |
| `gcp-dlp` | 710 ms | 266 ms | 40 / 41 | 4 / 7 | 25 / 25 |
| `gcp-vertex-ai` (Gemini) | 68.9 s | 6.0 s | 41 / 41 | 1 / 7 | 24 / 25 |

The 13 PII strings `local-rules` leaves behind are the person names and the free-form street addresses,
which have no shape to match; the chain with `local-ner` removes all 41, since the rules now cover the
three dates of birth, the UK postcode and the passport number. GCP DLP misses only the postcode and
Gemini misses nothing. The two non-PII strings the NER model removes are "Apple" in "Apple pie" and the
sign-off "The Support Desk", both tagged as organisations, which is the over-redaction to expect from
entity-based detection. Run `cargo test --release --test bench_dlp -- --ignored --nocapture` (see
`test-fixtures/bench-dlp/README.md`) to reproduce the table on your own machine and documents.

In short: use the local redacters when the data must not leave the machine or when you need a fast, cheap
first pass; use a cloud provider, ideally after the local pass, when coverage matters more than latency and cost.

## Multiple redacters

You can specify multiple redacters using `--redact` option multiple times.
The tool will apply redaction in the order of the redacters specified.
When multiple redacters are specified, the tool first tries to redact the file using the redacters that support input
files natively.
Only if such redacters are not available, the tool will try to redact the file using the redacters using conversions.

## PDF redaction

PDF redaction is supported by rendering PDF files as images and redacting them.
To render and convert PDF files the tool uses external library `Pdfium` (the C++ PDF library used by the Google Chromium
project).
This library needs to be installed separately on your system.

Installation instructions:

- Download the latest release from, for example,
  here [Pdfium releases](https://github.com/bblanchon/pdfium-binaries/releases) for your system.
- Extract the archive and copy library file `libpdfium.so` to the one of the following directory:
    - The path the redacter tool installed (such as `/usr/local/bin`)
    - The path that resides with redacter tool `/usr/local/lib/` if you have installed the tool in `/usr/local/bin`
    - The path with system libs like `/usr/lib/`.

If library is detected correctly it will be reported in the tool output as.
> PDF to image support: ✓ Yes

## Models and downloads

The OCR engine and the `local-ner` redacter need model files that are not bundled with the tool:

| Model | Files | Size | Source | Licence |
|---|---|---|---|---|
| `ocrs` (OCR) | `text-detection.rten`, `text-recognition.rten` | 11.7 MiB | https://ocrs-models.s3-accelerate.amazonaws.com/ | MIT OR Apache-2.0 (ocrs); weights trained on open, liberally licensed datasets |
| `distilbert-base-multilingual-cased-ner-hrl` (`local-ner`) | `model_uint8.onnx`, `tokenizer.json`, `config.json` | 131.6 MiB | https://huggingface.co/Xenova/distilbert-base-multilingual-cased-ner-hrl (revision `c2a4dbf`) | AFL-3.0 |

Nothing is downloaded without your consent. `--download-models` controls it:

- `ask` (default): when a run needs a model that is not installed and the tool runs in a terminal,
  it prints what it would download (files, size, source, licence, destination) and asks
  `Download now? [y/N]`. Without a terminal it behaves as `no`.
- `yes`: downloads without asking, for scripts and CI.
- `no`: never downloads; a missing model is reported with the manual installation instructions.

Downloaded files are verified against SHA-256 digests pinned in the tool and stored under
`~/.cache/redacter/models/<model>/` on Linux, `~/Library/Caches/redacter/models/<model>/` on macOS and
`%LOCALAPPDATA%\redacter\models\<model>\` on Windows. Only downloads are checksummed: a file already
in that directory is checked against its pinned size, and a copy you place in `models/<model>/` next
to the executable or in the legacy OCR directories is used as it is, checked against nothing.
`--models-dir <DIR>` (or the `REDACTER_MODELS_DIR` environment variable) moves that directory.
Downloads honour the `HTTPS_PROXY` environment variable.

For air-gapped machines, download the files listed above on another computer and copy them into
`<models dir>/<model>/`, for example `~/.cache/redacter/models/ocrs/text-detection.rten`. The tool
also looks in `models/<model>/` next to its own executable, and the OCR models are still found in
`~/.cache/ocrs` and `../share/ocrs` as in earlier versions.

## OCR

The tool supports OCR for images and PDF files using [ocrs engine](https://github.com/robertknight/ocrs).
The OCR models are downloaded on request or installed by hand as described in
[Models and downloads](#models-and-downloads); `~/.cache/ocrs` from earlier versions keeps working.

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

- Your file contents are sent to the DLP API for redaction. Make sure you trust the DLP API provider.
- The local redacters (`local-rules`, `local-ner`) send nothing anywhere. Their model files are
  fetched only on request, over HTTPS, and checked against SHA-256 digests pinned in the tool.
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
