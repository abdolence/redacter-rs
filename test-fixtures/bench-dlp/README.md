# bench-dlp

A small, fixed-corpus benchmark comparing the redacter CLI's providers on:

- **latency**: wall-clock time for `redacter cp` over the corpus, and per-file.
- **detection quality**: does each provider actually remove the PII in the
  corpus, and does it leave non-PII "control" text alone?

Providers compared: the offline ones (`local-rules`, `local-ner`, and the
chain `local-rules+local-ner`) against the cloud ones (`gcp-dlp`,
`gcp-vertex-ai`).

Not part of the `redacter` crate build; this is a measurement harness kept
under `test-fixtures/` for re-runs after future changes to the redacters, run
via the ignored integration test `tests/bench_dlp.rs`.

## What's measured

`truth.json` holds, per corpus file, three hand-written lists of exact
substrings:

- `pii`: strings that must be gone from the output -- person names, emails,
  phones, postal addresses, national ids, card/iban numbers, and dates of
  birth (as they actually appear in that file).
- `entities`: city, country and organisation names. These are reported as
  their own column (`loc/org redacted/total`) and are never judged pass/fail
  against `keep` -- whether a provider redacts a company or city name is a
  configuration choice, not a correctness bug.
- `keep`: non-PII control strings that must survive untouched -- non-DOB
  dates, order/ticket/invoice numbers, amounts, version-ish strings, and
  generic words. These exist to catch over-redaction.

A truth string counts as "gone" (a pii hit, a redacted entity, or a lost keep
string) when it is no longer present anywhere in the output file as an exact
substring. This is coarse by design: it does not check that the *right* span
was redacted, only whether the original text survived. See `score_text` in
`tests/bench_dlp.rs`.

`local-rules` is a regex/dictionary matcher: it is not expected to hit person
names, postal addresses or `entities` (it has no notion of named entities) --
only structured PII like emails, phones, card numbers and IDs.  `local-ner` is
a named-entity model: it is not expected to hit emails, phones, card numbers
or IDs (it has no notion of those formats) -- only names and, depending on
`--local-ner-entities`, organisations and locations. `local-rules+local-ner`
(the chain, run left to right) is the intended production configuration: it
is expected to cover both classes.

The same literal text can be PII in one file and a control in another on
purpose -- e.g. `"14 March 1985"` is a customer's date of birth in
`customer-note.txt` (`pii`) but an unrelated invoice date in `dates-en.txt`
(`keep`). That is the point: a context-free regex can't tell these apart,
which is exactly the kind of gap this benchmark is meant to surface.

## Corpus

Seven text-only fixtures (no images/PDF, to keep cloud cost and OCR variance
out of the comparison), copied from `test-fixtures/documents/` into a fresh
temp directory by name -- `tests/bench_dlp.rs` lists them explicitly rather
than scanning the directory, so `test-fixtures/documents/customer-form.pdf`
(and anything else added there later) is never pulled into the corpus:

- `customer-note.txt`, `customer.json`, `customer-profile.html`,
  `customers.csv`, `multilingual.txt`, `false-positives-en.txt`,
  `dates-en.txt`

Total corpus size is under 3 KB.

The corpus is text only, on purpose, so this benchmark says nothing about the
OCR path (images and PDFs read through the OCR engine before redaction): that
path has its own coverage, the `ci-ocr`-gated
`command_copy_local_rules_redacts_ocr_documents_test` in
`src/commands/copy_command.rs`, which redacts `customer-form.pdf` and
`form-example.png` and OCRs the output back to confirm no PII survives.

## Running it

Build a release binary, then run the ignored benchmark test with
`--nocapture` so its progress and summary table print to the terminal:

```sh
cargo build --release
cargo test --release --test bench_dlp -- --ignored --nocapture
```

Configuration is via environment variables:

- `BENCH_DLP_PROVIDERS`: comma-separated provider specs. Defaults to
  `local-rules,local-ner,local-rules+local-ner` (local providers only, no
  network calls).
- `BENCH_DLP_GCP_PROJECT`: GCP project id, required only when a `gcp-dlp` or
  `gcp-vertex-ai` spec is listed; the test fails fast with a clear message if
  it's missing.
- `BENCH_DLP_OUT`: output directory for run artifacts and `summary.md`.
  Defaults to `target/bench-dlp`.

Local providers only (the default):

```sh
cargo test --release --test bench_dlp -- --ignored --nocapture
```

All five providers, including the cloud ones (requires GCP credentials):

```sh
BENCH_DLP_PROVIDERS=local-rules,local-ner,local-rules+local-ner,gcp-dlp,gcp-vertex-ai \
BENCH_DLP_GCP_PROJECT=latestbit \
cargo test --release --test bench_dlp -- --ignored --nocapture
```

### What each run does

For every provider spec:

1. One warm-up pass over the whole corpus directory (untimed, discarded).
2. Three timed passes over the whole corpus directory (`std::time::Instant`
   around the `redacter cp` invocation) -- the median is reported.
3. One timed pass per corpus file, for per-file latency.

Results land in `<BENCH_DLP_OUT>/<provider>/` (scratch, not committed):
`run1/`..`run3/` and `perfile/<name>/` hold the redacted output, and
`<BENCH_DLP_OUT>/summary.md` holds the final table.

A provider spec joined with `+` (e.g. `local-rules+local-ner`) runs the chain
in a single `redacter cp -d local-rules -d local-ner ...` invocation, not as
separate passes.

### Cloud cost note

Each cloud provider spec is invoked exactly `1 + 3 + <file count>` times per
run (currently 7 corpus files, so 11 invocations). The test prints this count
before starting and aborts if it would ever be exceeded -- there is no retry
loop. Across those 11 invocations, the whole ~3 KB corpus is sent roughly 5
times over (4 whole-corpus passes' worth of bytes, plus the per-file passes
covering the corpus once more), so each cloud provider sees under 40 KB total
per run.

## Scoring

Scoring happens automatically at the end of the same test run: it reads
`truth.json`, scores each provider's `run1/` output, prints a markdown table,
and writes it to `<BENCH_DLP_OUT>/summary.md`:

| column | meaning |
|---|---|
| wall time (median of 3) | median of the 3 timed whole-corpus passes |
| per-file median | median of the per-file timed passes |
| pii hits/total | how many `pii` strings were actually removed |
| loc/org redacted/total | how many `entities` strings were removed (informational, not scored pass/fail) |
| keep survived/total | how many `keep` strings were left untouched |
| `[REDACTED]` count | total replacement tokens written across the corpus |

## Results

Local rows measured at commit `540fbc5` (2026-09-07,
`cargo test --release --test bench_dlp -- --ignored --nocapture`), cloud rows
from the full run at commit `3c3491e`; Intel(R) Core(TM) i7-10700K CPU @
3.80GHz (16 logical cores). All providers completed cleanly (11/11 calls made
each, no errors, no retries needed).

| provider | wall time (median of 3) | per-file median | pii hits/total | loc/org redacted/total | keep survived/total | [REDACTED] count |
|---|---|---|---|---|---|---|
| gcp-dlp | 710 ms | 266 ms | 40/41 | 4/7 | 25/25 | 73 |
| gcp-vertex-ai | 68901 ms | 5999 ms | 41/41 | 1/7 | 24/25 | 48 |
| local-ner | 370 ms | 144 ms | 15/41 | 7/7 | 23/25 | 32 |
| local-rules | 38 ms | 35 ms | 28/41 | 0/7 | 25/25 | 31 |
| local-rules+local-ner | 431 ms | 174 ms | 41/41 | 7/7 | 23/25 | 63 |

### Reading the numbers

`local-rules` is by far the fastest (about 20 ms) and now catches the
structured PII including the dates of birth, the postcode and the passport
number (28/41), missing only names and street addresses, while leaving every
control string and every city/org name untouched. `local-ner` alone inverts
that gap: it catches names and all seven `entities` but has no notion of
email/phone/card/id formats, so it lands at a low 15/41 pii hits.
`local-rules+local-ner`, the intended production configuration, is additive
on detection (41/41 pii hits, all entities) at essentially `local-ner`'s
latency (431 ms), since the regex pass is negligible next to the NER model.
The chain now matches `gcp-vertex-ai` on raw pii recall (41/41 both) and
beats `gcp-dlp` (40/41), and both cloud providers leave keep strings alone
almost as well, but neither is a drop-in replacement for the chain's
entity coverage (`gcp-dlp` 4/7, `gcp-vertex-ai` 1/7 loc/org redacted, versus
7/7 for `local-ner`/the chain); `gcp-dlp` is close to the chain's latency
(710 ms vs 431 ms) while `gcp-vertex-ai` is two orders of magnitude slower
(69 s median, up to 21 s for a single small file), the clear cost of routing
every file through an LLM.
