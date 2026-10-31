# bench-dlp

A benchmark of the redacters of the `redacter` CLI. One run measures detection quality and time
per file for every redacter that can be reached, over four datasets, and writes a markdown
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

A finding is on a text value when it contains the value, or lies inside it and spans at least
60% of it, and is at most 3 times as long as the value, ignoring case. A finding is on an
image box when it overlaps the box and is at most 3 times its area, and the boxes on a target
find it when together they cover at least 60% of it, since a value is often reported as one box
per word. Sensitive values, neutral values and keep strings are all judged by this rule.

The size cap keeps a finding that covers a whole sentence or page from being on every value in
it. A finding more than 3 times longer than a value, such as a whole `KEY=value` line around a
short password, does not find it. Without the cap a finding on an email would also find a
name inside the email, and 22% of the person values of the public dataset sit inside a longer
email or URL in the same record.

Findings are counted once per place: the same span or box reported twice, by two redacters of
a chain for instance, is one finding, and the same text at two places is two. A value that
occurs more than once in a file is found when any finding is on it. A box target counts as one
true finding however many word boxes find it.

A finding that is not a false alarm is left out of the precision: one on a neutral entry, and
one inside a sensitive entry that no finding finds, such as `Baker Street` from an address or
one word box of a name. A text finding is inside an entry when it is a part of it, a box when
at least 60% of its area lies in the entry.

| Column | Meaning |
|---|---|
| Recall | sensitive entries found, over all categories |
| Precision | findings on a found sensitive entry, over those and the false alarms. Shown where the truth lists everything sensitive: every dataset but `public` |
| Per category | found/total for person, contact, address, id_number, financial, date_of_birth, credential, network and other. `-` for a category the dataset has no entries in |
| Keep hit | keep strings that a finding is on, out of all keep strings. Shown where precision is |
| Whole run | wall time of one `scan` over the dataset |
| Per file | whole run over the number of files |
| Failed/blocked/unsupported | files `scan` could not read, files it lacked a converter for, and files no selected redacter reads. Their entries count as not found |

Recall is per truth category, whatever the redacter calls a finding: a redacter that finds an
email under any label has found the email.

Times include the start of the process and the load of the models of a local redacter, once
per run. The per-file time of a small dataset is therefore mostly that fixed cost, and the
figures are comparable between redacters on one dataset, not between datasets.

## Datasets

Select them with `BENCH_DLP_DATASETS`, a comma-separated list. The default is
`fixtures,secrets,public`.

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
- `public`: a fixed sample of 200 records of the public dataset described below, in
  `public.json`. Each record is one file. Each annotated span is cut out of the text by its
  character offsets and is a sensitive value, or a neutral one for the labels the fixtures
  treat as neutral (places, organisations, dates and times, occupation, language, employment
  and education). The labels map to the categories in `tests/bench_dlp/categories.rs`. The
  annotation is not exhaustive, so only recall is reported.
- `images`: `test-fixtures/media/form-example.png` and `test-fixtures/documents/customer-form.pdf`,
  the same customer form. `images.json` holds a pixel box for each value, measured on the
  PNG and, for the PDF, at the 1 pixel per point scale the PDF renderer uses. The local
  redacters need the OCR and PDF addons. `images.json` is in the format of `truth.json`, with a
  box in place of a text value.

## Public data

`public.json` is a sample of [nvidia/Nemotron-PII](https://huggingface.co/datasets/nvidia/Nemotron-PII)
by NVIDIA, licensed under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).

- source: the `test` split of 100,000 records, revision `b70ffaf5ff39e079776134c5bf4381f00a9fd1ed`
  (last modified 2025-12-17), taken in October 2026;
- sample: 10 windows of 20 consecutive records, the window `i` starting at record
  `i * (100000 - 20) / 10`;
- changes: the `text` and the `label` of every span are unchanged. A span is kept as `start` and
  `end` character offsets, and 15 of the 1,568 spans of the sample were left out because their
  offsets do not select their text.

## Running it

```sh
cargo build --release
cargo test --release --test bench_dlp -- --ignored --nocapture
```

The harness reads the addons from the place the binary does, `REDACTER_ADDONS_DIR` or the user
cache directory, and downloads nothing. Install the ones the run needs once:

```sh
redacter addons install local-ner,local-gliner,ocr,pdf
```

The default runs the local redacters over `fixtures,secrets,public`. A redacter whose addon is
missing is listed as failed, and a dataset whose addon is missing shows its files as blocked.
All datasets and every redacter that is configured:

```sh
BENCH_DLP_DATASETS=fixtures,secrets,public,images \
BENCH_DLP_PROVIDERS=local-rules,local-ner,local-gliner,local-rules+local-gliner,gcp-dlp,aws-comprehend \
BENCH_DLP_SCAN_ARGS="--gcp-project-id my-project --aws-region eu-north-1" \
cargo test --release --test bench_dlp -- --ignored --nocapture
```

Settings are environment variables:

| Variable | Meaning |
|---|---|
| `BENCH_DLP_PROVIDERS` | Comma-separated specs. A spec joined with `+` is a chain, run as one `redacter scan -d a -d b`. Default is the local redacters and their chains |
| `BENCH_DLP_DATASETS` | `fixtures`, `secrets`, `public`, `images`. Default is `fixtures,secrets,public` |
| `BENCH_DLP_SCAN_ARGS` | Options added to every `redacter scan`, split on whitespace, so a value cannot hold a space. They configure the cloud redacters: see `redacter scan --help` |
| `BENCH_DLP_OUT` | Output directory. Default is `target/bench-dlp` |

A spec for which `scan` writes no report is listed as failed with the `Error:` line of its
output, which names the setting or the addon a redacter lacks. It does not stop the run. Cloud
redacters bill for what they read, and each dataset is sent once.

## Output

`<BENCH_DLP_OUT>/summary.md` has an overview table, one cell per redacter and dataset with
recall, precision and time per file, and a table per dataset with the columns above.
`results.json` has the same numbers, with every category and the counts behind each
percentage. Both are scratch output and are not committed.
