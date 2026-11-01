# DLP comparison

Quality and speed of the redacters of `redacter`, measured with the benchmark harness in `test-fixtures/bench-dlp/` over four datasets. Eleven redacters and chains are covered: the local redacters, Presidio, GCP DLP, AWS Comprehend, AWS Bedrock Guardrails, GCP Vertex AI and AWS Bedrock. Every number on this page comes from the runs described under [How the numbers were measured](#how-the-numbers-were-measured).

## What was measured

The harness runs `redacter scan <dataset> -d <redacter>` and compares the findings in the JSON report with a list of what the dataset contains, called the truth. Nothing is read back from a redacted copy, so the numbers say what a redacter finds.

Each entry of the truth is one of three kinds:

- sensitive: a value, or for images a box, that must be found. Each has a category;
- neutral: a city or an organisation. Whether a redacter finds one is a configuration choice, so it counts neither for nor against;
- keep: a string that looks sensitive and is not, such as an order number or a commit hash.

The columns of the tables:

- Recall: the share of sensitive entries that a finding is on. A finding is on a text value when it contains the value, or lies inside it and covers at least 60% of it, and is at most 3 times as long as the value. A finding on an image is on a box when it overlaps the box and the boxes on a target together cover at least 60% of it;
- Precision: the share of findings that are on a sensitive entry rather than a false alarm. A finding on a neutral entry, or inside a sensitive entry that no finding is on, is not counted as a false alarm. Reported where the truth lists everything sensitive, which is every dataset except `public`;
- Keep hit: the keep strings that a finding is on, out of all keep strings. A lower number is better;
- A category column: sensitive entries found out of all of that category. `person` is names and user names, `contact` emails and phone numbers, `address` street addresses, postcodes and coordinates, `id_number` national, tax, customer and device identifiers, `financial` cards and bank accounts, `date_of_birth` dates of birth, `credential` passwords, API keys and PINs, `network` IP and MAC addresses and URLs, and `other` any sensitive value that fits none of these. A category is shown only for the datasets that have entries in it.

A chain is several redacters given to one scan with `-d a -d b`, written `a+b` here. Each reads the file on its own and the findings are merged, with the same span or box reported by both counted once.

The full scoring rules, including the size cap on findings and what counts as a false alarm, are in [test-fixtures/bench-dlp/README.md](../test-fixtures/bench-dlp/README.md).

## Datasets

- `fixtures`: eight text files from `test-fixtures/documents/` (text, JSON, HTML, CSV, German, French and Spanish, contextual PII, dates and words that look like PII). 48 sensitive values and 31 keep strings;
- `secrets`: a synthetic corpus of 41 planted secrets in 25 files (passwords in config files, vendor API keys, tokens, JWTs, private keys, connection strings), and four control files with strings that look like secrets and are not. 29 files, 19 keep strings. The values are made up, apart from the AWS documentation example key;
- `public`: a fixed sample of 200 records of [nvidia/Nemotron-PII](https://huggingface.co/datasets/nvidia/Nemotron-PII) by NVIDIA, licensed under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/). Each record is one file, 981 sensitive values in all. The sample is 10 windows of 20 consecutive records of the `test` split, and the text and label of every span are unchanged. The annotation is not exhaustive, so only recall is reported;
- `images`: a customer form as a PNG and as a PDF, 2 files and 16 boxes. The local redacters, Presidio, AWS Comprehend and AWS Bedrock Guardrails read an image through OCR, text recognition by the `ocr` addon. GCP DLP and the LLM redacters read the image themselves. See [Detection per redacter](../README.md#detection-per-redacter).

## Overview

Each cell is recall / precision / time per file. Precision is not reported for `public`.

| Redacter | Fixtures | Secrets | Public | Images |
|---|---|---|---|---|
| `local-rules` | 54% / 100% / 51 ms | 88% / 100% / 1 ms | 24% / - / 0 ms | 62% / 100% / 366 ms |
| `local-ner` | 23% / 69% / 40 ms | 0% / 0% / 18 ms | 23% / - / 39 ms | 19% / 100% / 334 ms |
| `local-gliner` | 100% / 94% / 443 ms | 2% / 33% / 146 ms | 55% / - / 441 ms | 88% / 100% / 658 ms |
| `local-rules+local-ner` | 77% / 89% / 43 ms | 88% / 97% / 19 ms | 46% / - / 38 ms | 94% / 100% / 352 ms |
| `local-rules+local-gliner` | 100% / 95% / 435 ms | 88% / 95% / 141 ms | 57% / - / 466 ms | 88% / 100% / 634 ms |
| `presidio` | 69% / 56% / 18 ms | 7% / 21% / 6 ms | 66% / - / 12 ms | 69% / 92% / 312 ms |
| `gcp-dlp` | 85% / 75% / 269 ms | 41% / 100% / 84 ms | 49% / - / 72 ms | 94% / 94% / 461 ms |
| `aws-comprehend` | 98% / 88% / 178 ms | 56% / 82% / 86 ms | 80% / - / 84 ms | 94% / 100% / 577 ms |
| `aws-bedrock-guardrails` | 90% / 96% / 238 ms | 46% / 83% / 157 ms | 74% / - / 161 ms | 81% / 100% / 602 ms |
| `gcp-vertex-ai` | 98% / 100% / 8073 ms | 100% / 95% / 8171 ms | 86% / - / 9062 ms | 100% / 100% / 16339 ms |
| `aws-bedrock` | 98% / 93% / 776 ms | 93% / 59% / 554 ms | 87% / - / 583 ms | 94% / 100% / 2109 ms |

The `gcp-vertex-ai` cell for `fixtures` is from a second run, see [How the numbers were measured](#how-the-numbers-were-measured). In the first run the same cell was 77% / 95% / 6785 ms, with 1 of 8 files failed.

## Fixtures

| Redacter | Recall | Precision | Person | Contact | Address | ID number | Financial | Date of birth | Other | Keep hit |
|---|---|---|---|---|---|---|---|---|---|---|
| `local-rules` | 54% | 100% | 0/13 | 18/18 | 1/8 | 2/2 | 2/2 | 3/4 | 0/1 | 0/31 |
| `local-ner` | 23% | 69% | 10/13 | 0/18 | 1/8 | 0/2 | 0/2 | 0/4 | 0/1 | 4/31 |
| `local-gliner` | 100% | 94% | 13/13 | 18/18 | 8/8 | 2/2 | 2/2 | 4/4 | 1/1 | 1/31 |
| `local-rules+local-ner` | 77% | 89% | 10/13 | 18/18 | 2/8 | 2/2 | 2/2 | 3/4 | 0/1 | 4/31 |
| `local-rules+local-gliner` | 100% | 95% | 13/13 | 18/18 | 8/8 | 2/2 | 2/2 | 4/4 | 1/1 | 1/31 |
| `presidio` | 69% | 56% | 10/13 | 18/18 | 0/8 | 0/2 | 2/2 | 3/4 | 0/1 | 13/31 |
| `gcp-dlp` | 85% | 75% | 11/13 | 18/18 | 5/8 | 2/2 | 2/2 | 3/4 | 0/1 | 0/31 |
| `aws-comprehend` | 98% | 88% | 13/13 | 18/18 | 8/8 | 2/2 | 2/2 | 4/4 | 0/1 | 5/31 |
| `aws-bedrock-guardrails` | 90% | 96% | 13/13 | 18/18 | 8/8 | 2/2 | 2/2 | 0/4 | 0/1 | 1/31 |
| `gcp-vertex-ai` | 98% | 100% | 13/13 | 18/18 | 8/8 | 2/2 | 2/2 | 4/4 | 0/1 | 0/31 |
| `aws-bedrock` | 98% | 93% | 13/13 | 18/18 | 8/8 | 2/2 | 2/2 | 4/4 | 0/1 | 3/31 |

- `local-gliner` and `local-rules+local-gliner` found every sensitive value. `aws-comprehend`, `gcp-vertex-ai` and `aws-bedrock` found 98%;
- `local-rules` found all 18 contact values and none of the 13 person values. `local-ner` found 10 of the 13 person values and none of the contact values;
- precision is lowest for `presidio` (56%), `local-ner` (69%) and `gcp-dlp` (75%). `presidio` is on 13 of the 31 keep strings, `aws-comprehend` on 5, and `local-rules`, `gcp-dlp` and `gcp-vertex-ai` on none;
- the single `other` value was found by the two GLiNER rows only;
- `aws-bedrock-guardrails` found none of the 4 dates of birth.

## Secrets

| Redacter | Recall | Precision | Credential | Keep hit |
|---|---|---|---|---|
| `local-rules` | 88% | 100% | 36/41 | 0/19 |
| `local-ner` | 0% | 0% | 0/41 | 0/19 |
| `local-gliner` | 2% | 33% | 1/41 | 0/19 |
| `local-rules+local-ner` | 88% | 97% | 36/41 | 0/19 |
| `local-rules+local-gliner` | 88% | 95% | 36/41 | 0/19 |
| `presidio` | 7% | 21% | 3/41 | 0/19 |
| `gcp-dlp` | 41% | 100% | 17/41 | 0/19 |
| `aws-comprehend` | 56% | 82% | 23/41 | 2/19 |
| `aws-bedrock-guardrails` | 46% | 83% | 19/41 | 2/19 |
| `gcp-vertex-ai` | 100% | 95% | 41/41 | 1/19 |
| `aws-bedrock` | 93% | 59% | 38/41 | 11/19 |

- `gcp-vertex-ai` found all 41 secrets, with 1 of the 19 keep strings hit;
- `local-rules` found 88% with no false alarm and no keep string hit. The chains with `local-ner` and `local-gliner` keep that recall and add false alarms (precision 97% and 95%);
- `aws-bedrock` found 93% at a precision of 59% and hit 11 of the 19 keep strings;
- `gcp-dlp`, `aws-comprehend` and `aws-bedrock-guardrails` found 41%, 56% and 46%;
- `local-ner` found none of the 41, `local-gliner` 1 and `presidio` 3.

## Public

Recall only, since the annotation does not list everything sensitive.

| Redacter | Recall | Person | Contact | Address | ID number | Financial | Date of birth | Credential | Network | Other |
|---|---|---|---|---|---|---|---|---|---|---|
| `local-rules` | 24% | 9/244 | 157/161 | 1/57 | 16/187 | 7/126 | 31/31 | 0/33 | 16/82 | 0/60 |
| `local-ner` | 23% | 199/244 | 3/161 | 21/57 | 0/187 | 0/126 | 0/31 | 0/33 | 0/82 | 4/60 |
| `local-gliner` | 55% | 234/244 | 158/161 | 46/57 | 17/187 | 37/126 | 31/31 | 0/33 | 10/82 | 2/60 |
| `local-rules+local-ner` | 46% | 201/244 | 157/161 | 22/57 | 16/187 | 7/126 | 31/31 | 0/33 | 16/82 | 4/60 |
| `local-rules+local-gliner` | 57% | 234/244 | 161/161 | 46/57 | 32/187 | 40/126 | 31/31 | 0/33 | 17/82 | 2/60 |
| `presidio` | 66% | 201/244 | 156/161 | 14/57 | 90/187 | 64/126 | 31/31 | 6/33 | 72/82 | 15/60 |
| `gcp-dlp` | 49% | 219/244 | 160/161 | 27/57 | 2/187 | 12/126 | 31/31 | 4/33 | 8/82 | 15/60 |
| `aws-comprehend` | 80% | 240/244 | 161/161 | 39/57 | 85/187 | 116/126 | 31/31 | 23/33 | 82/82 | 9/60 |
| `aws-bedrock-guardrails` | 74% | 236/244 | 160/161 | 44/57 | 55/187 | 114/126 | 0/31 | 23/33 | 82/82 | 10/60 |
| `gcp-vertex-ai` | 86% | 239/244 | 161/161 | 46/57 | 186/187 | 126/126 | 31/31 | 27/33 | 15/82 | 11/60 |
| `aws-bedrock` | 87% | 232/244 | 161/161 | 45/57 | 187/187 | 125/126 | 31/31 | 31/33 | 36/82 | 6/60 |

- `aws-bedrock` (87%) and `gcp-vertex-ai` (86%) have the highest recall, followed by `aws-comprehend` (80%) and `aws-bedrock-guardrails` (74%). The best local row is `local-rules+local-gliner` at 57%;
- on `person`, `aws-comprehend` found 240 of 244, `gcp-vertex-ai` 239 and `local-gliner` 234. `local-ner` found 199 and `local-rules` 9;
- on `id_number`, `aws-bedrock` found 187 of 187 and `gcp-vertex-ai` 186. `gcp-dlp` found 2, `local-rules` 16 and `aws-comprehend` 85;
- on `network`, `aws-comprehend` and `aws-bedrock-guardrails` found all 82, `presidio` 72, `aws-bedrock` 36, `gcp-vertex-ai` 15 and `gcp-dlp` 8;
- on `credential`, no local row found any of the 33, where `local-rules` found 36 of the 41 in `secrets`. `aws-bedrock` found 31 and `gcp-vertex-ai` 27;
- every redacter found all 31 dates of birth except `local-ner` and `aws-bedrock-guardrails`, which found none.

## Images

The truth is 16 boxes, so one box is about 6 percentage points of recall (derived).

| Redacter | Recall | Precision | Person | Contact | Address | ID number | Financial | Date of birth | Keep hit |
|---|---|---|---|---|---|---|---|---|---|
| `local-rules` | 62% | 100% | 0/4 | 4/4 | 0/2 | 2/2 | 2/2 | 2/2 | 0/2 |
| `local-ner` | 19% | 100% | 3/4 | 0/4 | 0/2 | 0/2 | 0/2 | 0/2 | 0/2 |
| `local-gliner` | 88% | 100% | 2/4 | 4/4 | 2/2 | 2/2 | 2/2 | 2/2 | 0/2 |
| `local-rules+local-ner` | 94% | 100% | 3/4 | 4/4 | 2/2 | 2/2 | 2/2 | 2/2 | 0/2 |
| `local-rules+local-gliner` | 88% | 100% | 2/4 | 4/4 | 2/2 | 2/2 | 2/2 | 2/2 | 0/2 |
| `presidio` | 69% | 92% | 3/4 | 4/4 | 0/2 | 0/2 | 2/2 | 2/2 | 0/2 |
| `gcp-dlp` | 94% | 94% | 4/4 | 4/4 | 2/2 | 1/2 | 2/2 | 2/2 | 0/2 |
| `aws-comprehend` | 94% | 100% | 3/4 | 4/4 | 2/2 | 2/2 | 2/2 | 2/2 | 0/2 |
| `aws-bedrock-guardrails` | 81% | 100% | 3/4 | 4/4 | 2/2 | 2/2 | 2/2 | 0/2 | 0/2 |
| `gcp-vertex-ai` | 100% | 100% | 4/4 | 4/4 | 2/2 | 2/2 | 2/2 | 2/2 | 0/2 |
| `aws-bedrock` | 94% | 100% | 3/4 | 4/4 | 2/2 | 2/2 | 2/2 | 2/2 | 0/2 |

- `gcp-vertex-ai` found all 16 boxes at a precision of 100%. `gcp-dlp`, `aws-comprehend`, `aws-bedrock` and `local-rules+local-ner` found 94%;
- `local-rules+local-ner` (94%) found more than `local-gliner` and `local-rules+local-gliner` (88%) here;
- `presidio` found 69%, `local-rules` 62% and `local-ner` 19%.

## Speed

Time per file in milliseconds: the wall time of one `scan` over the dataset divided by the number of files. It includes the start of the process and, for a local redacter, the load of its models once per run, so the time of a small dataset is largely that fixed cost. Compare the times within a dataset, not between datasets. For the cloud rows it includes the network round trip from the machine of the run.

| Redacter | Fixtures | Secrets | Public | Images |
|---|---|---|---|---|
| `local-rules` | 51 ms | 1 ms | 0 ms | 366 ms |
| `local-ner` | 40 ms | 18 ms | 39 ms | 334 ms |
| `local-gliner` | 443 ms | 146 ms | 441 ms | 658 ms |
| `local-rules+local-ner` | 43 ms | 19 ms | 38 ms | 352 ms |
| `local-rules+local-gliner` | 435 ms | 141 ms | 466 ms | 634 ms |
| `presidio` | 18 ms | 6 ms | 12 ms | 312 ms |
| `gcp-dlp` | 269 ms | 84 ms | 72 ms | 461 ms |
| `aws-comprehend` | 178 ms | 86 ms | 84 ms | 577 ms |
| `aws-bedrock-guardrails` | 238 ms | 157 ms | 161 ms | 602 ms |
| `gcp-vertex-ai` | 8073 ms | 8171 ms | 9062 ms | 16339 ms |
| `aws-bedrock` | 776 ms | 554 ms | 583 ms | 2109 ms |

The whole run over the 200 files of `public`, in milliseconds:

| Redacter | Whole run on public |
|---|---|
| `local-rules` | 62 ms |
| `local-ner` | 7878 ms |
| `local-gliner` | 88352 ms |
| `local-rules+local-ner` | 7797 ms |
| `local-rules+local-gliner` | 93374 ms |
| `presidio` | 2568 ms |
| `gcp-dlp` | 14526 ms |
| `aws-comprehend` | 16962 ms |
| `aws-bedrock-guardrails` | 32304 ms |
| `gcp-vertex-ai` | 1812407 ms |
| `aws-bedrock` | 116794 ms |

- on `public`, `local-rules` takes 0 ms per file, `local-ner` 39 ms, `presidio` 12 ms and `local-gliner` 441 ms;
- among the services, `gcp-dlp` takes 72 ms to 461 ms per file, `aws-comprehend` 84 ms to 577 ms and `aws-bedrock-guardrails` 157 ms to 602 ms;
- the LLM redacters are slowest: `aws-bedrock` takes 554 ms to 2109 ms per file and `gcp-vertex-ai` 8073 ms to 16339 ms. The whole run of `gcp-vertex-ai` over `public` was 1812407 ms, about 30 minutes (derived).

## Local or cloud

What leaves the machine, by redacter:

| Redacter | What is sent, and where |
|---|---|
| `local-rules`, `local-ner`, `local-gliner` and their chains | Nothing. Detection and OCR run on the machine |
| `presidio` | The text, to the Presidio analyzer you run. In this run it ran in a container on the same machine |
| `gcp-dlp` | The text, or the image itself, to the Cloud DLP API of the Google Cloud project |
| `aws-comprehend` | The text, or the text OCR read from an image, to Amazon Comprehend in `eu-west-1` |
| `aws-bedrock-guardrails` | The text, or the text OCR read from an image, to Bedrock Guardrails in `eu-north-1` |
| `gcp-vertex-ai` | The whole text, or the image, to the Gemini model on Vertex AI |
| `aws-bedrock` | The whole text, or the image, to Amazon Nova 2 Lite on Bedrock in `eu-north-1` |

With `cp`, chained redacters apply in the order given, so `-d local-rules -d gcp-dlp` finds the secrets and the pattern-shaped values locally before the rest is sent to GCP DLP. See [Multiple redacters](../README.md#multiple-redacters).

The cloud redacters are billed for what they read. Pricing is on the providers' pages: [Sensitive Data Protection](https://cloud.google.com/sensitive-data-protection/pricing), [Vertex AI](https://cloud.google.com/vertex-ai/generative-ai/pricing), [Amazon Comprehend](https://aws.amazon.com/comprehend/pricing/) and [Amazon Bedrock](https://aws.amazon.com/bedrock/pricing/), which includes Guardrails. The local redacters have no per-file cost.

## Limits

- `gemini-llm` (the Gemini API) and `open-ai-llm` were not measured. There were no credentials for them;
- the `secrets` corpus is synthetic: the values are made up and the files are templates assembled at run time;
- `fixtures` has 8 files and `images` has 2 files and 16 boxes, so a difference of a few points is a few values;
- each cell is one run. The LLM redacters do not answer the same way twice: the `gcp-vertex-ai` cell for `fixtures` was 77% recall with 1 failed file in the first run and 98% with none in the second;
- every redacter ran with its default configuration: the default label lists and rules of the local redacters, Presidio's default recognizers, and the default model of the LLM redacters (`publishers/google/models/gemini-3.8-flash` on Vertex AI, Amazon Nova 2 Lite on Bedrock). A tuned configuration, such as more labels for `local-gliner`, a custom info type list for `gcp-dlp` or your own `local-rules` rules, changes the numbers;
- `aws-bedrock-guardrails` ran with a guardrail that has all 31 built-in PII types set to anonymise;
- times are from one machine and one network, and include process start and model load;
- `public` has no precision, since its annotation is not exhaustive;
- `fixtures` is the only dataset with deliberately multilingual text, in German, French and Spanish. `aws-comprehend` reads English text only.

## How the numbers were measured

- commit `2390a25` of this repository, release build;
- a Mac mini with an Apple M6 chip, on 2026-11-01. The cloud services were called from that machine;
- the first run covers the local redacters and their chains, `presidio`, `gcp-dlp` and `gcp-vertex-ai`; a second run covers `aws-comprehend`, and a third `aws-bedrock-guardrails` and `aws-bedrock`. The `gcp-vertex-ai` cell for `fixtures` is from a fourth run of that one cell, because 1 of its 8 files failed in the first run; the second run had no failed file and its numbers are used;
- Presidio is the analyzer image 2.2.366 and the image redactor image 0.0.62 from `ghcr.io/data-privacy-stack`, run as containers on the same machine. The image redactor has its own version line, so there is no 2.2.366 tag for it;
- no file failed, was blocked or was unsupported in any cell used here.

## Reproducing

Build the release binary, install the addons the local redacters and the images dataset need, and run the harness. The full list of settings is in [test-fixtures/bench-dlp/README.md](../test-fixtures/bench-dlp/README.md).

```sh
cargo build --release
redacter addons install local-ner,local-gliner,ocr,pdf
```

Run 1, with Presidio started first:

```sh
docker run -d -p 5002:3000 ghcr.io/data-privacy-stack/presidio-analyzer:2.2.366
docker run -d -p 5003:3000 ghcr.io/data-privacy-stack/presidio-image-redactor:0.0.62

BENCH_DLP_DATASETS=fixtures,secrets,public,images \
BENCH_DLP_PROVIDERS="local-rules,local-ner,local-gliner,local-rules+local-ner,local-rules+local-gliner,gcp-dlp,gcp-vertex-ai,presidio" \
BENCH_DLP_SCAN_ARGS="--gcp-project-id <project> --gcp-region global --presidio-text-analyze-url http://localhost:5002/analyze --presidio-image-redact-url http://localhost:5003/redact" \
BENCH_DLP_OUT=run-1 \
cargo test --release --test bench_dlp -- --ignored --nocapture
```

Run 2, AWS Comprehend:

```sh
BENCH_DLP_DATASETS=fixtures,secrets,public,images \
BENCH_DLP_PROVIDERS=aws-comprehend \
BENCH_DLP_SCAN_ARGS="--aws-region eu-west-1" \
BENCH_DLP_OUT=run-2 \
cargo test --release --test bench_dlp -- --ignored --nocapture
```

Run 3, AWS Bedrock Guardrails and AWS Bedrock, with a guardrail that has all 31 built-in PII types set to anonymise:

```sh
BENCH_DLP_DATASETS=fixtures,secrets,public,images \
BENCH_DLP_PROVIDERS="aws-bedrock-guardrails,aws-bedrock" \
BENCH_DLP_SCAN_ARGS="--aws-region eu-north-1 --aws-bedrock-guardrail-id <guardrail id> --aws-bedrock-guardrail-version DRAFT" \
BENCH_DLP_OUT=run-3 \
cargo test --release --test bench_dlp -- --ignored --nocapture
```

Run 4, the `gcp-vertex-ai` cell for `fixtures`:

```sh
BENCH_DLP_PROVIDERS=gcp-vertex-ai \
BENCH_DLP_DATASETS=fixtures \
BENCH_DLP_SCAN_ARGS="--gcp-project-id <project> --gcp-region global" \
BENCH_DLP_OUT=run-4 \
cargo test --release --test bench_dlp -- --ignored --nocapture
```

Each run writes `summary.md` and `results.json` to its `BENCH_DLP_OUT` directory. Cloud redacters bill for what they read, and each dataset is sent once per redacter.
