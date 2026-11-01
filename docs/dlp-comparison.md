# DLP comparison

Quality and speed of the redacters of `redacter`, measured with the benchmark harness in `test-fixtures/bench-dlp/` over four datasets. Thirteen redacters and chains are covered: the local redacters, Presidio, GCP DLP, AWS Comprehend, AWS Bedrock Guardrails, GCP Vertex AI and AWS Bedrock. Every number on this page comes from the runs described under [How the numbers were measured](#how-the-numbers-were-measured).

## What was measured

The harness runs `redacter scan <dataset> -d <redacter>` and compares the findings in the JSON report with a list of what the dataset contains, called the truth. Nothing is read back from a redacted copy, so the numbers say what a redacter finds.

Each entry of the truth is one of three kinds:

- sensitive: a value, or for images a box, that must be found. Each has a category;
- neutral: a city or an organisation, and in `public` also a date, time, occupation, language, employment status or education level. Whether a redacter finds one is a configuration choice, so it counts neither for nor against;
- keep: a string that looks sensitive and is not, such as an order number or a commit hash.

The columns of the tables:

- Recall: the share of sensitive entries that a finding is on. A finding is on a text value when it contains the value, or lies inside it and spans at least 60% of it, and is at most 3 times as long as the value. A finding is on an image box when it overlaps the box and is at most 3 times its area, and the boxes on a target find it when together they cover at least 60% of it;
- Precision: the share of findings that are on a found sensitive entry rather than a false alarm. A finding on a neutral entry, or inside a sensitive entry that no finding finds (one word box of a name, say), is not counted as a false alarm. Reported where the truth lists everything sensitive, which is every dataset except `public`;
- Keep hit: the keep strings that a finding is on, out of all keep strings. A lower number is better;
- A category column: sensitive entries found out of all of that category. `person` is names and user names, `contact` emails and phone numbers, `address` street addresses, postcodes and coordinates, `id_number` national, tax, customer and device identifiers, `financial` cards and bank accounts, `date_of_birth` dates of birth, `credential` passwords, API keys and PINs, `network` IP and MAC addresses and URLs, and `other` any sensitive value that fits none of these. A category is shown only for the datasets that have entries in it.

A chain is several redacters given to one scan with `-d a -d b`, written `a+b` here. Each reads the file on its own and the findings are merged, with the same span or box reported by both counted once.

The full scoring rules are in [test-fixtures/bench-dlp/README.md](../test-fixtures/bench-dlp/README.md).

## Datasets

- `fixtures`: eight text files from `test-fixtures/documents/` (text, JSON, HTML, CSV, German, French and Spanish, contextual PII, dates and words that look like PII). 48 sensitive values and 31 keep strings;
- `secrets`: a synthetic corpus of 41 planted secrets in 25 files (passwords in config files, vendor API keys, tokens, JWTs, private keys, connection strings), and four control files with strings that look like secrets and are not. 29 files, 19 keep strings. The values are made up, apart from the AWS documentation example key;
- `public`: a fixed sample of 200 records of [nvidia/Nemotron-PII](https://huggingface.co/datasets/nvidia/Nemotron-PII) by NVIDIA, licensed under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/). Each record is one file, 981 sensitive values in all. The sample is 10 windows of 20 consecutive records of the `test` split. The text and label of every span are unchanged, and 15 of the 1,568 spans of the sample are left out because their offsets do not select their text. The annotation is not exhaustive;
- `images`: a customer form as a PNG and as a PDF, 2 files and 16 boxes. The local redacters, Presidio, AWS Comprehend and AWS Bedrock Guardrails read an image through OCR, text recognition by the `ocr` addon. GCP DLP and the LLM redacters read the image themselves. See [Detection per redacter](../README.md#detection-per-redacter).

## Overview

Each cell is recall / precision / time per file. Time per file is described under [Speed](#speed). Precision is not reported for `public`.

| Redacter | Fixtures | Secrets | Public | Images |
|---|---|---|---|---|
| `local-rules` | 54% / 100% / 51 ms | 93% / 100% / 1 ms | 24% / - / under 1 ms | 62% / 100% / 366 ms |
| `local-ner` | 23% / 69% / 40 ms | 0% / 0% / 18 ms | 23% / - / 39 ms | 19% / 100% / 334 ms |
| `local-gliner` | 100% / 94% / 443 ms | 2% / 33% / 146 ms | 55% / - / 441 ms | 88% / 100% / 658 ms |
| `local-rules+local-ner` | 77% / 89% / 43 ms | 93% / 97% / 25 ms | 46% / - / 38 ms | 94% / 100% / 352 ms |
| `local-rules+local-gliner` | 100% / 95% / 435 ms | 93% / 95% / 177 ms | 57% / - / 466 ms | 88% / 100% / 634 ms |
| `local-credential-ml` | 0% / - / 77 ms | 61% / 100% / 10 ms | 2% / - / 28 ms | 0% / - / 389 ms |
| `local-rules+local-credential-ml` | 54% / 100% / 30 ms | 95% / 100% / 15 ms | 25% / - / 28 ms | 62% / 100% / 340 ms |
| `presidio` | 69% / 56% / 18 ms | 7% / 21% / 6 ms | 66% / - / 12 ms | 69% / 92% / 312 ms |
| `gcp-dlp` | 85% / 75% / 269 ms | 41% / 100% / 84 ms | 49% / - / 72 ms | 94% / 94% / 461 ms |
| `aws-comprehend` | 98% / 88% / 178 ms | 56% / 82% / 86 ms | 80% / - / 84 ms | 94% / 100% / 577 ms |
| `aws-bedrock-guardrails` | 90% / 96% / 238 ms | 46% / 83% / 157 ms | 74% / - / 161 ms | 81% / 100% / 602 ms |
| `gcp-vertex-ai` | 98% / 100% / 8073 ms | 100% / 95% / 8171 ms | 86% / - / 9062 ms | 100% / 100% / 16339 ms |
| `aws-bedrock` | 98% / 93% / 776 ms | 93% / 59% / 554 ms | 87% / - / 583 ms | 94% / 100% / 2109 ms |

The `gcp-vertex-ai` cell for `fixtures` is from the re-run described under [How the numbers were measured](#how-the-numbers-were-measured).

## Fixtures

| Redacter | Recall | Precision | Person | Contact | Address | ID number | Financial | Date of birth | Other | Keep hit |
|---|---|---|---|---|---|---|---|---|---|---|
| `local-rules` | 54% | 100% | 0/13 | 18/18 | 1/8 | 2/2 | 2/2 | 3/4 | 0/1 | 0/31 |
| `local-ner` | 23% | 69% | 10/13 | 0/18 | 1/8 | 0/2 | 0/2 | 0/4 | 0/1 | 4/31 |
| `local-gliner` | 100% | 94% | 13/13 | 18/18 | 8/8 | 2/2 | 2/2 | 4/4 | 1/1 | 1/31 |
| `local-rules+local-ner` | 77% | 89% | 10/13 | 18/18 | 2/8 | 2/2 | 2/2 | 3/4 | 0/1 | 4/31 |
| `local-rules+local-gliner` | 100% | 95% | 13/13 | 18/18 | 8/8 | 2/2 | 2/2 | 4/4 | 1/1 | 1/31 |
| `local-credential-ml` | 0% | - | 0/13 | 0/18 | 0/8 | 0/2 | 0/2 | 0/4 | 0/1 | 0/31 |
| `local-rules+local-credential-ml` | 54% | 100% | 0/13 | 18/18 | 1/8 | 2/2 | 2/2 | 3/4 | 0/1 | 0/31 |
| `presidio` | 69% | 56% | 10/13 | 18/18 | 0/8 | 0/2 | 2/2 | 3/4 | 0/1 | 13/31 |
| `gcp-dlp` | 85% | 75% | 11/13 | 18/18 | 5/8 | 2/2 | 2/2 | 3/4 | 0/1 | 0/31 |
| `aws-comprehend` | 98% | 88% | 13/13 | 18/18 | 8/8 | 2/2 | 2/2 | 4/4 | 0/1 | 5/31 |
| `aws-bedrock-guardrails` | 90% | 96% | 13/13 | 18/18 | 8/8 | 2/2 | 2/2 | 0/4 | 0/1 | 1/31 |
| `gcp-vertex-ai` | 98% | 100% | 13/13 | 18/18 | 8/8 | 2/2 | 2/2 | 4/4 | 0/1 | 0/31 |
| `aws-bedrock` | 98% | 93% | 13/13 | 18/18 | 8/8 | 2/2 | 2/2 | 4/4 | 0/1 | 3/31 |

- `local-rules` found all 18 contact values and none of the 13 person values. `local-ner` found 10 of the 13 person values and none of the contact values;
- the single `other` value was found by the two GLiNER rows only;
- `aws-bedrock-guardrails` found none of the 4 dates of birth.

## Secrets

| Redacter | Recall | Precision | Credential | Keep hit |
|---|---|---|---|---|
| `local-rules` | 93% | 100% | 38/41 | 0/19 |
| `local-ner` | 0% | 0% | 0/41 | 0/19 |
| `local-gliner` | 2% | 33% | 1/41 | 0/19 |
| `local-rules+local-ner` | 93% | 97% | 38/41 | 0/19 |
| `local-rules+local-gliner` | 93% | 95% | 38/41 | 0/19 |
| `local-credential-ml` | 61% | 100% | 25/41 | 0/19 |
| `local-rules+local-credential-ml` | 95% | 100% | 39/41 | 0/19 |
| `presidio` | 7% | 21% | 3/41 | 0/19 |
| `gcp-dlp` | 41% | 100% | 17/41 | 0/19 |
| `aws-comprehend` | 56% | 82% | 23/41 | 2/19 |
| `aws-bedrock-guardrails` | 46% | 83% | 19/41 | 2/19 |
| `gcp-vertex-ai` | 100% | 95% | 41/41 | 1/19 |
| `aws-bedrock` | 93% | 59% | 38/41 | 11/19 |

- `local-rules` found 93% with no false alarm and no keep string hit. The chains with `local-ner` and `local-gliner` keep that recall and add false alarms (precision 97% and 95%);
- `local-credential-ml` alone found 61% with no false alarm and no keep string hit, and chained after `local-rules` 95% (39 of 41). The two it and `local-rules` both miss are the cookie value and the `.netrc` password. With `--local-credential-ml-min-score 0.5` the two rows found 76% (31 of 41) and 100% (41 of 41), again with 100% precision and no keep string hit;

## Public

Recall only, since the annotation does not list everything sensitive.

| Redacter | Recall | Person | Contact | Address | ID number | Financial | Date of birth | Credential | Network | Other |
|---|---|---|---|---|---|---|---|---|---|---|
| `local-rules` | 24% | 9/244 | 157/161 | 1/57 | 16/187 | 7/126 | 31/31 | 0/33 | 16/82 | 0/60 |
| `local-ner` | 23% | 199/244 | 3/161 | 21/57 | 0/187 | 0/126 | 0/31 | 0/33 | 0/82 | 4/60 |
| `local-gliner` | 55% | 234/244 | 158/161 | 46/57 | 17/187 | 37/126 | 31/31 | 0/33 | 10/82 | 2/60 |
| `local-rules+local-ner` | 46% | 201/244 | 157/161 | 22/57 | 16/187 | 7/126 | 31/31 | 0/33 | 16/82 | 4/60 |
| `local-rules+local-gliner` | 57% | 234/244 | 161/161 | 46/57 | 32/187 | 40/126 | 31/31 | 0/33 | 17/82 | 2/60 |
| `local-credential-ml` | 2% | 2/244 | 5/161 | 0/57 | 0/187 | 1/126 | 0/31 | 6/33 | 5/82 | 0/60 |
| `local-rules+local-credential-ml` | 25% | 9/244 | 157/161 | 1/57 | 16/187 | 8/126 | 31/31 | 6/33 | 17/82 | 0/60 |
| `presidio` | 66% | 201/244 | 156/161 | 14/57 | 90/187 | 64/126 | 31/31 | 6/33 | 72/82 | 15/60 |
| `gcp-dlp` | 49% | 219/244 | 160/161 | 27/57 | 2/187 | 12/126 | 31/31 | 4/33 | 8/82 | 15/60 |
| `aws-comprehend` | 80% | 240/244 | 161/161 | 39/57 | 85/187 | 116/126 | 31/31 | 23/33 | 82/82 | 9/60 |
| `aws-bedrock-guardrails` | 74% | 236/244 | 160/161 | 44/57 | 55/187 | 114/126 | 0/31 | 23/33 | 82/82 | 10/60 |
| `gcp-vertex-ai` | 86% | 239/244 | 161/161 | 46/57 | 186/187 | 126/126 | 31/31 | 27/33 | 15/82 | 11/60 |
| `aws-bedrock` | 87% | 232/244 | 161/161 | 45/57 | 187/187 | 125/126 | 31/31 | 31/33 | 36/82 | 6/60 |

- on `credential`, `local-credential-ml` found 6 of the 33 and no other local redacter found any, where `local-rules` found 38 of the 41 in `secrets`. `aws-bedrock` found 31 and `gcp-vertex-ai` 27;
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
| `local-credential-ml` | 0% | - | 0/4 | 0/4 | 0/2 | 0/2 | 0/2 | 0/2 | 0/2 |
| `local-rules+local-credential-ml` | 62% | 100% | 0/4 | 4/4 | 0/2 | 2/2 | 2/2 | 2/2 | 0/2 |
| `presidio` | 69% | 92% | 3/4 | 4/4 | 0/2 | 0/2 | 2/2 | 2/2 | 0/2 |
| `gcp-dlp` | 94% | 94% | 4/4 | 4/4 | 2/2 | 1/2 | 2/2 | 2/2 | 0/2 |
| `aws-comprehend` | 94% | 100% | 3/4 | 4/4 | 2/2 | 2/2 | 2/2 | 2/2 | 0/2 |
| `aws-bedrock-guardrails` | 81% | 100% | 3/4 | 4/4 | 2/2 | 2/2 | 2/2 | 0/2 | 0/2 |
| `gcp-vertex-ai` | 100% | 100% | 4/4 | 4/4 | 2/2 | 2/2 | 2/2 | 2/2 | 0/2 |
| `aws-bedrock` | 94% | 100% | 3/4 | 4/4 | 2/2 | 2/2 | 2/2 | 2/2 | 0/2 |

- the 94% of `local-rules+local-ner` against the 88% of `local-gliner` is one box of 16.

## Speed

Time per file, in the overview, is the wall time of one `scan` over the dataset divided by the number of files. It includes the start of the process and, for a local redacter, the load of its models. Compare the times within a dataset, not between datasets. For the cloud rows it includes the network round trip from the machine of the run.

The whole run over the 200 files of `public`, in milliseconds:

| Redacter | Whole run on public |
|---|---|
| `local-rules` | 62 ms |
| `local-ner` | 7878 ms |
| `local-gliner` | 88352 ms |
| `local-rules+local-ner` | 7797 ms |
| `local-rules+local-gliner` | 93374 ms |
| `local-credential-ml` | 5738 ms |
| `local-rules+local-credential-ml` | 5771 ms |
| `presidio` | 2568 ms |
| `gcp-dlp` | 14526 ms |
| `aws-comprehend` | 16962 ms |
| `aws-bedrock-guardrails` | 32304 ms |
| `gcp-vertex-ai` | 1812407 ms |
| `aws-bedrock` | 116794 ms |

The whole run of `gcp-vertex-ai` over `public` was about 30 minutes (derived).

## Local or cloud

What leaves the machine, by redacter:

| Redacter | What is sent, and where |
|---|---|
| `local-rules`, `local-ner`, `local-gliner`, `local-credential-ml` and their chains | Nothing. Detection and OCR run on the machine |
| `presidio` | The text, to the Presidio analyzer you run. With `cp`, images and PDF pages go to the Presidio image redactor you run. In this run the analyzer ran in a container on the same machine |
| `gcp-dlp` | The text, or the image itself, to the Cloud DLP API of the Google Cloud project |
| `aws-comprehend` | The text, or the text OCR read from an image, to Amazon Comprehend in the region set with `--aws-region` (`eu-west-1` in this run) |
| `aws-bedrock-guardrails` | The text, or the text OCR read from an image, to Bedrock Guardrails in the region set with `--aws-region` (`eu-north-1` in this run) |
| `gcp-vertex-ai` | The whole text, or the image, to the Gemini model on Vertex AI, in the location set with `--gcp-region` (`global` in this run) |
| `aws-bedrock` | The whole text, or the image, to Amazon Nova 2 Lite on Bedrock, through the `eu.` cross-region inference profile, an AWS alias that lets any EU region serve the request (`--aws-region` was `eu-north-1` in this run) |

With `cp`, chained redacters apply in the order given, so for a text file `-d local-rules -d gcp-dlp` redacts the secrets and the pattern-shaped values locally before the rest is sent to GCP DLP. An image is read natively by GCP DLP, and a PDF is rendered to page images that it reads natively, so both go to GCP DLP whole and `local-rules` does not run on them. See [Multiple redacters](../README.md#multiple-redacters).

The cloud redacters are billed for what they read. Pricing is on the providers' pages: [Sensitive Data Protection](https://cloud.google.com/sensitive-data-protection/pricing), [Vertex AI](https://cloud.google.com/vertex-ai/generative-ai/pricing), [Amazon Comprehend](https://aws.amazon.com/comprehend/pricing/) and [Amazon Bedrock](https://aws.amazon.com/bedrock/pricing/), which includes Guardrails. The local redacters have no per-file cost.

## Limits

- `gemini-llm` (the Gemini API) and `open-ai-llm` were not measured;
- the `secrets` corpus is synthetic: the values are made up;
- the `local-credential-ml` rows are one run each of the default configuration, with the threshold 0.74. Every word of a flagged line with a digit or one of the symbols `!@#%^&*+=?~` is redacted, and no dataset here is built to test what that takes along;
- `fixtures` has 8 files and `images` has 2 files and 16 boxes, so a difference of a few points is a few values;
- each cell is one run, and an LLM redacter can answer differently for the same files: the re-run of the `gcp-vertex-ai` cell for `fixtures` differed in precision and in keep hits, see [How the numbers were measured](#how-the-numbers-were-measured);
- the `local-rules` time on `fixtures` (412 ms for the run, 51 ms per file) is from the first scan of the first run. It is higher than the times of `local-ner` (40 ms) and `local-rules+local-ner` (43 ms) on the same files;
- the `local-credential-ml` time on `fixtures` (616 ms for the run, 77 ms per file) is from the first scan of the run. It is higher than the time of `local-rules+local-credential-ml` (30 ms) on the same files;
- every redacter ran with its default configuration: the default label lists and rules of the local redacters, Presidio's default recognizers, and the default model of the LLM redacters (`publishers/google/models/gemini-3.8-flash` on Vertex AI, Amazon Nova 2 Lite on Bedrock). A tuned configuration, such as more labels for `local-gliner`, a custom info type list for `gcp-dlp` or your own `local-rules` rules, changes the numbers;
- `aws-bedrock-guardrails` ran with a [guardrail](../README.md#aws-bedrock-guardrails) that has all 31 built-in PII types set to anonymise;
- times are from one machine and one network;
- `fixtures` is the only dataset with deliberately multilingual text, in German, French and Spanish. `aws-comprehend` reads English text only.

## How the numbers were measured

- commit `2390a25` of this repository, release build;
- the `local-credential-ml` rows come from a later run, in the same way: commit `c71f738`, release build, the same machine, on 2026-11-01;
- the `secrets` cells of the local redacters and their chains come from a run on commit `19874f5` (the `generic-api-key` rule), in the same way;
- a Mac mini with an Apple M6 chip, on 2026-11-01. The cloud services were called from that machine;
- the first run covers the local redacters and their chains, `presidio`, `gcp-dlp` and `gcp-vertex-ai`; a second run covers `aws-comprehend`, and a third `aws-bedrock-guardrails` and `aws-bedrock`;
- the re-run repeated the `gcp-vertex-ai` cell for `fixtures`, because 1 of its 8 files failed in the first run, which counts as not found. The first run gave 77% recall, 95% precision, 2 of 31 keep strings hit and 6785 ms per file. The re-run had no failed file and gave 98%, 100%, 0 of 31 and 8073 ms, and its numbers are used;
- Presidio is the analyzer image 2.2.366 from `ghcr.io/data-privacy-stack`, run as a container on the same machine;
- no file failed, was blocked or was unsupported in any cell used here.

## Reproducing

Build the release binary, install the addons the local redacters and the images dataset need, and run the harness. The full list of settings is in [test-fixtures/bench-dlp/README.md](../test-fixtures/bench-dlp/README.md).

```sh
cargo build --release
redacter addons install local-ner,local-gliner,local-credential-ml,ocr,pdf
```

Run 1, with the Presidio analyzer started first:

```sh
docker run -d -p 5002:3000 ghcr.io/data-privacy-stack/presidio-analyzer:2.2.366

BENCH_DLP_DATASETS=fixtures,secrets,public,images \
BENCH_DLP_PROVIDERS="local-rules,local-ner,local-gliner,local-rules+local-ner,local-rules+local-gliner,gcp-dlp,gcp-vertex-ai,presidio" \
BENCH_DLP_SCAN_ARGS="--gcp-project-id <project> --gcp-region global --presidio-text-analyze-url http://localhost:5002/analyze" \
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

Run 3, AWS Bedrock Guardrails and AWS Bedrock:

```sh
BENCH_DLP_DATASETS=fixtures,secrets,public,images \
BENCH_DLP_PROVIDERS="aws-bedrock-guardrails,aws-bedrock" \
BENCH_DLP_SCAN_ARGS="--aws-region eu-north-1 --aws-bedrock-guardrail-id <guardrail-id> --aws-bedrock-guardrail-version DRAFT" \
BENCH_DLP_OUT=run-3 \
cargo test --release --test bench_dlp -- --ignored --nocapture
```

The re-run is run 1 with `BENCH_DLP_PROVIDERS=gcp-vertex-ai` and `BENCH_DLP_DATASETS=fixtures`.

The `local-credential-ml` rows are run 4, on commit `c71f738`:

```sh
BENCH_DLP_DATASETS=fixtures,secrets,public,images \
BENCH_DLP_PROVIDERS="local-credential-ml,local-rules+local-credential-ml" \
BENCH_DLP_OUT=run-4 \
cargo test --release --test bench_dlp -- --ignored --nocapture
```

The figures at the threshold 0.50 add `BENCH_DLP_SCAN_ARGS="--local-credential-ml-min-score 0.5"` and `BENCH_DLP_DATASETS=secrets`.

Each run writes `summary.md` and `results.json` to its `BENCH_DLP_OUT` directory.
