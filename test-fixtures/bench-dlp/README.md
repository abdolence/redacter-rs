# bench-dlp

A small, fixed-corpus benchmark comparing the redacter CLI's providers on:

- **latency**: wall-clock time for `redacter cp` over the corpus, and per-file.
- **detection quality**: does each provider actually remove the PII in the
  corpus, and does it leave non-PII "control" text alone?

Providers compared: the offline ones (`local-rules`, `local-ner`, `local-gliner`,
and the chains `local-rules+local-ner` and `local-rules+local-gliner`) against
the cloud ones (`gcp-dlp`, `gcp-vertex-ai`).

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
`--local-ner-entities`, organisations and locations. `local-gliner` is a
zero-shot span model driven by free-text labels (`--local-gliner-labels`):
it is expected to cover both classes on its own, since its default label list
includes both structured PII and person/address labels, but it has no notion
of an entity type it was not given a label for. `local-rules+local-ner` and
`local-rules+local-gliner` (each a chain, run left to right) are production
configurations: they are expected to cover both classes.

The same literal text can be PII in one file and a control in another on
purpose -- e.g. `"14 March 1985"` is a customer's date of birth in
`customer-note.txt` (`pii`) but an unrelated invoice date in `dates-en.txt`
(`keep`). That is the point: a context-free regex can't tell these apart,
which is exactly the kind of gap this benchmark is meant to surface.

## Corpus

Eight text-only fixtures (no images/PDF, to keep cloud cost and OCR variance
out of the comparison), copied from `test-fixtures/documents/` into a fresh
temp directory by name -- `tests/bench_dlp.rs` lists them explicitly rather
than scanning the directory, so `test-fixtures/documents/customer-form.pdf`
(and anything else added there later) is never pulled into the corpus:

- `customer-note.txt`, `customer.json`, `customer-profile.html`,
  `customers.csv`, `multilingual.txt`, `false-positives-en.txt`,
  `dates-en.txt`, `contextual-en.txt`

`contextual-en.txt` is a call summary written so that every PII item needs
context rather than shape or capitalisation to find: a lower-case name
(`jonas petersen`), a free-form address with no postcode keyword, a medical
condition, a keyword-less date of birth (`12/03/1984`, indistinguishable by
shape from a plain date), a forum handle and a username. It also carries two
non-PII controls that read like an organisation out of context (`Apple
Watch`, `The Support Desk`) to check for over-redaction by entity-based
providers. `local-rules` and plain `local-ner` are not expected to find most
of this file's PII; it exists to show what a context-aware provider adds over
the regex/NER baseline.

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
  `local-rules,local-ner,local-rules+local-ner,local-gliner,local-rules+local-gliner`
  (local providers only, no network calls).
- `BENCH_DLP_GCP_PROJECT`: GCP project id, required only when a `gcp-dlp` or
  `gcp-vertex-ai` spec is listed; the test fails fast with a clear message if
  it's missing.
- `BENCH_DLP_OUT`: output directory for run artifacts and `summary.md`.
  Defaults to `target/bench-dlp`.

Local providers only (the default):

```sh
cargo test --release --test bench_dlp -- --ignored --nocapture
```

All seven providers, including the cloud ones (requires GCP credentials):

```sh
BENCH_DLP_PROVIDERS=local-rules,local-ner,local-rules+local-ner,local-gliner,local-rules+local-gliner,gcp-dlp,gcp-vertex-ai \
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
run (currently 8 corpus files, so 12 invocations). The test prints this count
before starting and aborts if it would ever be exceeded -- there is no retry
loop. Across those 12 invocations, the whole ~4 KB corpus is sent roughly 5
times over (4 whole-corpus passes' worth of bytes, plus the per-file passes
covering the corpus once more), so each cloud provider sees under 45 KB total
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

Local rows measured at commit `257e8eb` (2026-09-08,
`cargo test --release --test bench_dlp -- --ignored --nocapture`) on the
8-file corpus (48 pii / 8 entities / 31 keep); Intel(R) Core(TM) i7-10700K
CPU @ 3.80GHz (16 logical cores). All five local providers completed cleanly
(12/12 calls made each, no errors, no retries needed).

Cloud rows are kept from the 7-file corpus run at commit `3c3491e`
(2026-09-07, 41 pii / 7 entities / 25 keep, before `contextual-en.txt` joined
the corpus) and were not re-run this round; their pii/entities/keep totals
are out of 41/7/25, not the 48/8/31 the local rows use below.

| provider | wall time (median of 3) | per-file median | pii hits/total | loc/org redacted/total | keep survived/total | [REDACTED] count |
|---|---|---|---|---|---|---|
| gcp-dlp (7-file corpus) | 710 ms | 266 ms | 40/41 | 4/7 | 25/25 | 73 |
| gcp-vertex-ai (7-file corpus) | 68901 ms | 5999 ms | 41/41 | 1/7 | 24/25 | 48 |
| local-rules | 51 ms | 49 ms | 28/48 | 0/8 | 31/31 | 31 |
| local-ner | 409 ms | 146 ms | 17/48 | 8/8 | 27/31 | 37 |
| local-rules+local-ner | 468 ms | 192 ms | 43/48 | 8/8 | 27/31 | 68 |
| local-gliner | 2357 ms | 784 ms | 45/48 | 1/8 | 28/31 | 51 |
| local-rules+local-gliner | 2492 ms | 823 ms | 48/48 | 1/8 | 27/31 | 59 |

### Reading the numbers

`local-rules` is by far the fastest (about 50 ms) and catches the structured
PII including the dates of birth, the postcode and the passport number
(28/48), missing the names and free-form addresses that have no shape to
match, while leaving every control string alone (31/31 keep). `local-ner`
alone inverts that gap: it catches names and all eight `entities` but has no
notion of email/phone/card/id formats or of context, so it lands at 17/48 pii
hits and misses three of the contextual fixture's context-only items.
`local-rules+local-ner` is additive on the structured/named split (43/48 pii,
all entities) at essentially `local-ner`'s latency (468 ms), but is still
short of the full 48 because neither half of the chain reads context: the
keyword-less date of birth and the free-form address in `contextual-en.txt`
need more than shape or a name-shaped token to find.

`local-gliner` on its own reaches 45/48 pii at default settings (a 14-label
PII-only list, no `organization`) and, chained after `local-rules`, reaches
48/48 -- every PII string in the corpus, including the lower-case name,
medical condition, keyword-less date of birth and the two handles in
`contextual-en.txt` that neither `local-rules` nor `local-ner` find. It costs
about 5x `local-rules+local-ner`'s latency (2492 ms vs 468 ms wall,
mostly one-time model load repeated per invocation in this per-process
harness) and, since `organization` is off by default, redacts only 1 of 8
`entities` (the address's town name, tagged under the `address` label) rather
than `local-ner`'s 8/8 -- entity coverage costs an extra label and the false
positives that come with it (see the `local-gliner` section of the top-level
README). Compared to the cloud rows on the smaller 7-file corpus,
`local-rules+local-gliner`'s 48/48 pii on the harder 8-file corpus is not
directly comparable, but the contextual fixture is exactly the kind of gap
`gcp-vertex-ai`'s LLM-based redaction is included here to cover; a like-for-like
comparison needs the cloud providers re-run on the current corpus.
