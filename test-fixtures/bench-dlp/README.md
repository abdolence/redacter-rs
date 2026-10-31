# bench-dlp

A benchmark of the redacters of the `redacter` CLI. One run measures detection quality and
latency for every redacter that can be reached, over four datasets, and writes a markdown
summary and a JSON file with the numbers.

The harness is the ignored integration test `tests/bench_dlp/main.rs`. It is a measurement
tool kept next to the fixtures, not part of the `redacter` build.

## How a run is scored

The harness runs `redacter scan <dataset> -d <redacter> --show-values --report-file ...` and
scores the findings in the JSON report against a truth. Nothing is read back from a redacted
copy, so the numbers say what a redacter finds, not what a copy happens to remove.

A truth has three kinds of entry:

- sensitive: a value, or for images a box, that must be found. Each has a category;
- neutral: a city or an organisation. Whether a redacter finds one is a configuration choice,
  so it counts neither for nor against;
- keep: a string that looks sensitive and is not, such as an order number, a commit hash or
  `password_min_length`.

A finding is on a text value when either text contains the other, ignoring case. A finding is
on an image box when it covers at least 60% of the box and is at most 3 times its area. A
value that occurs more than once in a file is found when any finding is on it.

| column | meaning |
|---|---|
| recall | sensitive entries found, over all categories |
| precision | findings on a sensitive entry, over all findings that are not on a neutral entry |
| per category | found/total for person, contact, address, id, financial, dob (date of birth), credential, network and other |
| keep hit | keep strings that a finding is on, out of all keep strings |
| whole run | wall time of one `scan` over the dataset, including the load of a local model |
| per-file median | median wall time of one `scan` per file, for datasets of at most 30 files |
| failed/blocked/unsupported | files `scan` could not read, files it lacked a converter for, and files no selected redacter reads. Their entries count as not found |

Recall is per truth category, whatever the redacter calls a finding: a redacter that finds an
email under any label has found the email. Precision counts a finding that is on no entry as a
false alarm, so it is only meaningful where the truth is complete.

## Datasets

Select them with `BENCH_DLP_DATASETS`, a comma-separated list. The default is
`fixtures,secrets`.

- `fixtures`: eight text files from `test-fixtures/documents/` (text, JSON, HTML, CSV, German,
  French and Spanish, contextual PII, dates and words that look like PII), listed in
  `truth.json` with a category for each value. The same literal can be sensitive in one file
  and a keep string in another: `14 March 1985` is a date of birth in `customer-note.txt` and
  an invoice date in `dates-en.txt`.
- `secrets`: a synthetic corpus of 41 planted secrets in 25 files (passwords in config files,
  vendor API keys, tokens, JWTs, private keys, connection strings), and four control files
  with strings that look like secrets and are not. Files are read with
  `--unsupported-as-text`, since `.env` files and keys have no text media type. The values
  are made up, apart from the AWS documentation example key. `secrets/secrets.json` stores
  each value in two parts and each file as a template over `{{value id}}`, and the harness
  assembles the files at run time, so that no committed line is a complete token that GitHub
  push protection would block. Every value a template names is a credential to find.
- `public`: a seeded sample of records of [nvidia/Nemotron-PII](https://huggingface.co/datasets/nvidia/Nemotron-PII)
  (CC BY 4.0), fetched at run time through the Hugging Face datasets server, in windows of 20
  rows at seeded offsets. `BENCH_DLP_DATASET_SAMPLE` sets the number of records, 200 by
  default. The sample is cached in `BENCH_DLP_OUT` and never committed. Each record is one
  file, and each annotated span is a sensitive value. Spans are matched by text, and a span
  whose text is not in the record is dropped. Labels map to the categories in
  `tests/bench_dlp/categories.rs`; job titles, languages, times and health or belief
  attributes are `other`. The annotation is not exhaustive, so precision on this dataset is a
  lower bound.
- `images`: `test-fixtures/media/form-example.png` and `test-fixtures/documents/customer-form.pdf`,
  the same customer form. `images.json` holds a pixel box for each value, measured on the
  PNG and, for the PDF, at the 1 pixel per point scale the PDF renderer uses. The local
  redacters need the OCR and PDF addons.

## Running it

```sh
cargo build --release
cargo test --release --test bench_dlp -- --ignored --nocapture
```

The default runs the local redacters over `fixtures,secrets` and makes no network call. All
datasets and every redacter that is configured:

```sh
BENCH_DLP_DATASETS=fixtures,secrets,public,images \
BENCH_DLP_PROVIDERS=local-rules,local-ner,local-gliner,local-rules+local-gliner,gcp-dlp,gcp-vertex-ai,aws-comprehend \
BENCH_DLP_GCP_PROJECT=my-project \
BENCH_DLP_AWS_REGION=eu-north-1 \
cargo test --release --test bench_dlp -- --ignored --nocapture
```

Settings are environment variables:

| variable | meaning |
|---|---|
| `BENCH_DLP_PROVIDERS` | Comma-separated specs. A spec joined with `+` is a chain, run as one `redacter scan -d a -d b`. Default is the local redacters and their chains |
| `BENCH_DLP_DATASETS` | `fixtures`, `secrets`, `public`, `images`. Default is `fixtures,secrets` |
| `BENCH_DLP_DATASET_SAMPLE` | Records of the public dataset. Default is 200 |
| `BENCH_DLP_OUT` | Output directory. Default is `target/bench-dlp` |
| `BENCH_DLP_ADDONS_DIR` | Addons directory. Default is `<BENCH_DLP_OUT>/addons`. The harness runs `redacter addons install` into it for the OCR and PDF addons and the models the specs use, which downloads anything missing |
| `BENCH_DLP_GCP_PROJECT` | GCP project id, for `gcp-dlp`, `gcp-vertex-ai` and `gemini-llm` |
| `BENCH_DLP_GCP_REGION` | Vertex AI location, `global` when unset |
| `BENCH_DLP_AWS_REGION` | AWS region, for `aws-comprehend`, `aws-bedrock` and `aws-bedrock-guardrails`. `AWS_REGION` is read too |
| `BENCH_DLP_AWS_GUARDRAIL_ID` | Guardrail id, for `aws-bedrock-guardrails` |
| `BENCH_DLP_PRESIDIO_ANALYZER_URL` | Presidio analyzer URL, for `presidio`; `ms-presidio` is accepted as its former name |
| `BENCH_DLP_PRESIDIO_IMAGE_URL` | Presidio image redactor URL, for images |
| `BENCH_DLP_OPENAI_API_KEY` | OpenAI API key, for `open-ai-llm` |

A spec whose settings are missing is listed as not measured with the variable it needs. A spec
for which `scan` writes no report is listed as failed with the last line of its error output.
Neither stops the run. Cloud redacters bill for what they read: a cloud spec sends each file
twice on a dataset of up to 30 files, once in the scored run and once for the per-file time,
and the public sample once.

## Output

`<BENCH_DLP_OUT>/summary.md` has an overview table, one cell per redacter and dataset with
recall and precision, and a table per dataset with the columns above. `results.json` has the
same numbers, with every category and the counts behind each percentage. Both are scratch
output and are not committed.

Timings are one cold run each: the load of a local model is part of every `scan`.
