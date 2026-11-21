# Reports

The files that `cp` and `scan` write with `--report-file`. `--report-format` selects JSON (the default), text, SARIF or OCSF, see [Command line options](../README.md#command-line-options) and [Scan command](../README.md#scan-command).

## Copy report

The JSON report of `cp` is one object with the counters `files_copied`, `files_redacted` and `files_skipped`, and
`files`, an entry for each file the copy met, in the order it met them. Files that `--max-size-limit` or
`--filename-filter` leave out of a directory listing are only counted in `files_skipped` and have no entry. With
`--categories` the report also has `categories`, the list of the categories the copy was limited to, such as
`["contact", "secrets"]`. A copy without `--categories`, or with every category, has no such field.

An entry has the fields `path`, `mime_type`, `status`, `reason` and `redactions`:

- `status` is `redacted`, `copied`, `copied_unredacted`, `skipped` or `failed`. `copied` is a file copied as it is
  because no redacter was selected, and `copied_unredacted` a file copied as it is because
  `--allow-unsupported-copies` is set;
- `reason` is `null` for a redacted or copied file and says why for the others;
- `redactions` lists what the redacters replaced in the file, sorted by location as in the scan report. A file with
  the status `redacted` and no redactions went through the redacters unchanged.

A redaction has the fields `redacter`, `label`, `score`, `location` and `value`, as a [finding of `scan`](#json-report)
has. The `value` is always masked, since `cp` has no `--show-values`, see [Masking](../README.md#masking). The `location` has the
`kind` `text`, `table_cell` or `image` of the scan report, and two more:

- `page`: the `page` of a PDF file, counted from 1, when the position on it is not known;
- `file`: the file as a whole, when the position in it is not known.

How exactly a redaction is located depends on the redacter and the content:

| Content | Location | Redacters |
|---|---|---|
| Text | Exact span: `start`, `end`, `line` and `column` | `local-rules`, `local-ner`, `local-gliner`, `local-credential-ml`, `aws-comprehend` and `presidio` report the span they replaced. `aws-bedrock-guardrails` does when its answer gives readable locations; the LLM redacters do with `--categories` |
| Text rewritten by the redacter | The `file` | `gcp-dlp`, the LLM redacters without `--categories`, and `aws-bedrock-guardrails` when its answer gives no readable locations return only the new text |
| Table | Cell: `row`, `column` and the span in the cell | `local-rules`, `local-ner`, `local-gliner` and `local-credential-ml`. `gcp-dlp` reports a table at the `file`; other redacters read a CSV file as text |
| Image or PDF page read with OCR | Page and box in pixels, around the words OCR blacked out | Any redacter that reads text |
| Image read by the redacter | Page and box in pixels | `gcp-dlp`, and the LLM redacters in the `coords` image mode, or in the default `auto` mode when the native edit is not available |
| Image edited by the redacter | The `page` of a PDF, or the `file` | `presidio`, and the LLM redacters in the `native` image mode, or in the `auto` mode when the native edit succeeds |

A text or table redaction located at the `file`, or at the `page` of a PDF, has the label `sensitive_data`, no score and
no value. Each step of `gcp-dlp`, or of an LLM redacter without `--categories`, on a text or a table that changes more
than whitespace is one such redaction, and so is every later step on the same text or table. So is a step that changes
the shape of a table. The boxes of an image step that follows a step that changed the size of the image are located at
the `page` or the `file` too, and keep their label, score and value; the step that changed the size still reports its
boxes.

A copy without `--report-file`, or with `--report-format text`, places nothing and takes what a copy without reports
takes.

## JSON report

`--report-file <FILE>` writes the report to a file in JSON by default; `--report-format` selects `text`, `sarif` or `ocsf`. The terminal always shows the usual output,
whatever the report options are; there is no mode that prints the JSON to stdout.

The report is one JSON object with the counters of the run and an entry for each file:

- `files_scanned`, `files_with_findings`, `files_unconfirmed`, `files_skipped`, `files_unsupported`, `files_blocked`
  and `files_failed`: the number of files in each state. `files_unconfirmed` counts the scanned files without findings
  that have `unlocated_values`, and is part of `files_scanned`;
- `files_ignored`: the number of files the [ignore file](../README.md#ignoring-files-and-findings) ignored. They have no entry in
  `files`;
- `findings`: the number of findings, and `findings_by_label`: the same count for each label;
- `findings_ignored`: the number of findings ignored by the `redacter:allow` marker or by block markers, not counted in
  `findings`;
- `files`: one entry for each file with its `path`, `mime_type`, `status`, `reason`, `unlocated_values` and `findings`.
  A file whose [block markers](ignoring-findings.md#block-markers) do not pair up also has `marker_warnings`, a list of the `line` of each
  marker and its `problem`: `unclosed_begin`, `stray_end` or `nested_begin`;
- `categories`: the categories `--categories` limited the scan to, such as `["contact", "secrets"]`. The field is
  left out when the scan looked for every category.

A finding has the fields `redacter`, `label`, `score`, `location` and `value`.

`files_scanned` counts the files the redacters read, and these count the files they did not:

- `files_skipped`: files left out by `--max-size-limit` or `--filename-filter`. Files that the filters leave out of a
  directory listing are only counted here, and have no entry in `files`;
- `files_unsupported`: files that no selected redacter reads, including image formats that OCR cannot read, such as
  SVG;
- `files_blocked`: files that need a converter the run lacks, such as OCR or PDF rendering;
- `files_failed`: files that could not be read or scanned.

The `status` of a file is `scanned`, `unsupported`, `blocked`, `skipped` or `failed`, and its `reason` is `null` for a
scanned file and says why for the others. `unlocated_values` counts the values an LLM redacter or AWS Bedrock Guardrails
named that are not in the file as written, such as a value the model reformatted. The values are not reported. A
scanned file without `findings` and with `unlocated_values` above 0 is the unconfirmed file of the terminal, and the row
of any file with such values shows the count as `2 values not located`: the file may hold more than its findings.

The `findings` of a file are sorted by location, as in the terminal, and the `score` is `null` when the redacter gives
none. The `value` is masked unless `--show-values` is set. The `location` has a `kind`:

- `text`: `start` and `end` are byte offsets in the text of the file, and `line` and `column` are the position of
  `start`, both counted from 1, the column in characters;
- `table_cell`: the `row` and `column` of the cell, both counted from 1, and the byte offsets `start` and `end` in the
  cell;
- `image`: the `page` of a PDF file counted from 1 (`null` for an image), and the box `y1`, `x1`, `y2`, `x2` in pixels.

Fields are only ever added to the report, never renamed or removed.

## Text report

`--report-format text` writes what the terminal shows for the files and the summary, without colours. The rows are wide
enough for the longest path, so no path is shortened. With `--categories` the summary line of `scan` ends with
`· Categories: contact, secrets`. The text report of `cp` starts with the line `Categories: contact, secrets`.

## SARIF report

`--report-format sarif` writes the report as [SARIF](https://docs.oasis-open.org/sarif/sarif/v2.1.0/sarif-v2.1.0.html)
2.1.0, the format GitHub code scanning reads, so the findings show in the code scanning tab and as annotations on pull
requests. `scan` and `cp` both write it, and a redaction of `cp` is a finding here.

| SARIF | What `scan` and `cp` write |
|---|---|
| `tool.driver` | `redacter`, its version and the repository URL |
| rule | One for each redacter and label found, with the `ruleId` `<redacter>/<label>`, such as `local-rules/email`, and `redacter/unconfirmed` for unconfirmed files |
| `level` | `error` for a finding, `warning` for an unconfirmed file |
| text finding | A `region` with `startLine`, `startColumn`, `endLine` and `endColumn`, counted from 1; `endColumn` is the column after the last character |
| table cell | No region, since the line of a cell in its file is not known; the message gives the row and column |
| image or PDF page | No region; the message gives the page and the box in pixels |
| a page of a PDF, or the whole file | No region; the message gives the page, if there is one, and has no value |
| unconfirmed file | One result for the file, without a region |
| failed, blocked and unsupported files | `invocations[0].toolExecutionNotifications` with the reason; `executionSuccessful` is `false` when a file failed or is blocked |
| skipped files | Not reported |
| `partialFingerprints` | `primaryLocationLineHash`, see below |
| `properties` of the run | `categories`, the categories `--categories` limited the run to; left out when the run looked for every category |

Columns count characters (Unicode code points), as in the other reports, and the run says so with
`columnKind` `unicodeCodePoints`. GitHub does not document which unit it counts columns in. GitHub shows a result under the file of its location, and a pull request annotation
needs a line in the diff, so annotations appear for text findings only.

A path is relative to the working directory, with `uriBaseId` `%SRCROOT%`, which GitHub resolves against the root of the
repository: run the command from the root of the checkout. A path outside the working directory is a `file:///` URI, and a
file of a `gs://`, `s3://` or `zip://` source is a URI of that scheme.

The message of a result has the label, the redacter, the score when there is one, and the masked value. SARIF always
holds the masked value: `--show-values` does not apply to it, since the report is stored by GitHub and shown to everyone
who can read the alerts. A result without a value, such as a page or a file that a redacter changed without saying where,
has no value in its message.

The SARIF report of `cp` has a result for each redaction of the [copy report](#copy-report) and these for the other
files:

- a file copied or skipped without being redacted, which includes every file of a copy without a redacter, is
  `unsupported`, or `blocked` when OCR or the PDF renderer is missing, as for `scan`;
- a file left out by `--max-size-limit` or `--filename-filter` is not reported;
- a file with the status `failed` is `failed`;
- a file with the status `redacted` and no redactions has no result.

The fingerprint is a SHA-256 hash of the rule, the path and the masked value, followed by `:` and the number of earlier
findings with the same three. The value itself is not part of it, since a hash of a secret can be reversed by trying
every candidate. It does not change when lines are added above a finding, so the alert stays the same alert. GitHub
shows at most 5,000 results of a run, the most severe first, and rejects a file with more than 25,000 results in a run.
The SARIF report of `cp` and of `scan` keeps 5,000 results: the most severe first, and within a severity
the first result of every rule before the second of any, so a rule with few results is not crowded out by one with
many. It says in a notification of the run how many it left out. The JSON report always lists all of them.

GitHub tells the analyses of one tool apart by the `category` of the upload, and an upload replaces the earlier one with
the same `category`. `scan` and `cp` write the same tool, so a workflow that uploads both sets a different `category`
for each.

A workflow that uploads the findings to code scanning without failing the job:

```yaml
name: Scan for secrets
on: [push, pull_request]

jobs:
  scan:
    runs-on: ubuntu-latest
    permissions:
      contents: read
      security-events: write
    steps:
      - uses: actions/checkout@v7
      - name: Install redacter
        run: |
          curl -fsSL https://raw.githubusercontent.com/abdolence/redacter-rs/master/install.sh | sh
          echo "$HOME/.local/bin" >> "$GITHUB_PATH"
      - name: Scan
        run: >
          redacter scan . -d local-rules --local-rules secrets,url --unsupported-as-text
          --report-file redacter.sarif --report-format sarif --fail-on never
      - uses: github/codeql-action/upload-sarif@v4
        if: always()
        with:
          sarif_file: redacter.sarif
          category: redacter-scan
```

## OCSF report

`--report-format ocsf` writes the report as [OCSF](https://schema.ocsf.io/1.9.0/) 1.9.0, the Open Cybersecurity Schema
Framework, a vendor-neutral schema for security events. Use it to send the findings to a SIEM or a security data lake.
`scan` and `cp` both write it, and a redaction of `cp` is a finding here.

The file is NDJSON, newline-delimited JSON: one JSON object on each line and no enclosing array, so a log shipper can
read it line by line. Every line is an event of the class Data Security Finding (`class_uid` 2006, `type_uid` 200601),
one for each file that has findings or is unconfirmed. A run without such a file writes an empty file. A clean,
skipped, unsupported, blocked or failed file has no event: the [JSON report](#json-report) and the exit code report
them.

| OCSF | What `scan` and `cp` write |
|---|---|
| `metadata` | `version` `1.9.0` and the `product`: `redacter`, its version and the repository URL |
| `time`, `finding_info.created_time` | When the report was written, in milliseconds since the Unix epoch |
| `finding_info.uid` | The URI of the file, so a file has the same `uid` in every run; the value is not part of it |
| `metadata.uid` | A UUID version 7 that is new for every event, so the events of two runs never share it |
| `file` | `name`, the full `path` (the URI for a file of a remote source) and the full `uri`, a `file:///` URI for a local file |
| `data_security.discovery_details` | One entry for each redacter, label and masked value in the file: `type` is the label, `rule.name` the redacter, `value` the masked value, `count` the number of findings |
| text finding | An `occurrences` entry with `start_line`, `end_line` and `column_number`, counted from 1 |
| table cell | An `occurrences` entry with `row_number` and `column_number`. The row counts data rows, so with a header row the first data row is 1 and the row in a spreadsheet is one more, as in the JSON and SARIF reports |
| image or PDF page | An `occurrences` entry with `page_number` when there is a page |
| `unmapped.redacter.findings` | One entry for each listed finding: its `discovery_detail`, the index in `discovery_details`, its `occurrence`, the index in the `occurrences` of that detail (left out when the finding has none), its `location` as the [JSON report](#json-report) gives it (byte offsets, the box in pixels of an image), and the `score` as the redacter gave it |
| `unmapped.redacter.listing_limit` | Present when an event lists fewer findings than the file holds: `omitted_findings` and `max_findings_per_detail` |
| unconfirmed file | An event with the severity Informational, no `data_security`, and `unmapped.redacter.unlocated_values` |

An event lists the first 1000 findings of each discovery detail, in the order of the file, with their `occurrences` and
`unmapped.redacter.findings` entries. The `count` of a detail is always exact, and `unmapped.redacter.listing_limit`
says how many findings the event leaves out and what the limit is. A file that holds one value thousands of times then
writes an event of bounded size.

The values are masked as in the other reports, and `--show-values` does not apply: an OCSF report is stored with a long
retention and read by many. There is no hash of a value in it.

The severity and the confidence of an event come from its findings:

| Attribute | Value |
|---|---|
| `severity_id` | High (4) when the file has a credential: a finding in the `credential` category, such as one of the `secrets` or `url-credentials` rules of `local-rules`, one of `local-credential-ml`, or a `PASSWORD` of GCP DLP. Medium (3) for any other finding. Informational (1) for an unconfirmed file |
| `confidence_score` | The highest score of the findings in the file, from 0 to 100; left out when no finding has a score |
| `confidence_id` | Low (1) below 50, Medium (2) from 50 to 79, High (3) from 80; Unknown (0) when there is no score, which is the case for rule-based and LLM redacters and for unconfirmed files |

Amazon Security Lake accepts custom sources in OCSF 1.3 or earlier only, as Parquet files, so this report is
not accepted there as it is.
