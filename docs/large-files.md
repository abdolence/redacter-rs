# Large files

How `cp` and `scan` read a file that is too large to hold in memory or to send in one request, and the memory and time it takes. The summary is in [Copy report](reports.md#copy-report) and [Scan command](../README.md#scan-command).

## Copy

A text file of more than about 4 MiB is redacted in pieces when every selected redacter is `local-rules` or
`local-credential-ml`, and each piece is written to the destination as it is finished. This covers plain text and logs,
JSON, YAML, XML, HTML, CSS and Markdown. A piece is read with 64 KiB of text on
each side, in windows of at most 4 MiB. Each redacter keeps its mask, and a value that crosses the end of a piece is
masked whole. A value of up to 128 KiB across lines, such as a private key block, is found wherever a piece ends, and a
longer one that crosses the end of a piece can be missed. With several redacters, each reads the text the ones before it
left, so the first reads the contexts of all of them. A line longer than a window is cut inside.

The output is the same as when the file is read whole, with two exceptions. A rule of your own that is anchored with `^`
or `$` and has no `(?m)` can match at the edge of a piece, and masks more than it does in a whole read. A redacter that
finds part of the mask of an earlier one masks the original value in its place, where a whole read leaves a broken mask.

Every redaction of a large file read in pieces is located, in the report too. The LLM redacters, `local-ner`,
`local-gliner` and `--sampling-size` read the file whole. A model read in pieces loses recall and precision on the
bench, see [Large files and the models](dlp-comparison.md#large-files-and-the-models).
`cp -d local-rules` on a 319 MB log with 26 findings peaks near 81 MiB of memory, against 695 MiB in 0.21, in about 3%
more time. The same copy of a 366 MB log with 5.2 million findings peaks near 423 MiB on macOS, against 1.7 GiB in 0.21.
The system allocator of macOS keeps freed memory, so the peak it reports can be several times what the run needs.

With a JSON, SARIF or OCSF report, the redactions of a copy are kept in a temporary file instead of in memory, the one
`scan` keeps its findings in. The file is in the system temporary directory, has no name, is removed when the copy ends,
however it ends, and on Unix is readable by the current user only. It holds values masked. A copy that cannot write to
it stops with an error that names the directory.

`cp -d local-rules --report-file r.json` on a 23 MB log with 700 000 redactions peaks near 317 MiB on macOS. The
temporary file adds 10 to 20% to the time of that copy.
Building the SARIF report keeps at most 5000 results for each rule in memory, and building the
OCSF report keeps an event for each file.

`aws-comprehend`, `presidio`, `gcp-dlp` and `aws-bedrock-guardrails` read a text in pieces when it does not fit in one
request of the provider, where it fails otherwise. A text that fits in one request is sent as it is and redacted by the
provider as before. A longer text is cut into pieces that fill a request, read with 1 KiB of text on each side. Four
requests are in flight at a time for `aws-comprehend` and `gcp-dlp`, and one for `presidio` and `aws-bedrock-guardrails`.
With a local redacter in the chain, at most two requests are in flight. A cloud redacter before the local one, as in
`-d gcp-dlp -d local-rules`, is sent the 64 KiB of context of the local redacter on each side of a piece, which the
provider bills as well. With the local redacter first, as in `-d local-rules -d gcp-dlp`, the provider is sent 1 KiB.
The provider only finds the values here, and each is masked as the provider masks it: one `X` for each character for `aws-comprehend` and `presidio`, `[REDACTED]` for `gcp-dlp`, and the type or the name of the regex
in braces, such as `{NAME}`, for `aws-bedrock-guardrails`. The rest of the text is copied as it is, and a value of up to
2 KiB across the end of a piece is masked whole. A provider reads a piece without the rest of the text, so it can find
other values in it than in a whole text. A request of `gcp-dlp` that reaches the limit of 3000 findings is sent again as
its two halves. Each request holds at most:

| Redacter | Request | Limit it follows |
|---|---|---|
| `aws-comprehend` | 100 000 bytes | The 100 KB document of `DetectPiiEntities` |
| `presidio` | 512 KiB | The 1 000 000 characters that spaCy accepts in `nlp.max_length`, which the analyzer refuses above; a character takes at least one byte |
| `gcp-dlp` | 480 000 bytes | The 0.5 MB request, less the fields around the text |
| `aws-bedrock-guardrails` | 100 000 bytes | 106 text units of 1000 characters, the least a Region takes in a request for the sensitive information policy |

The 64 KiB context of `local-rules` and `local-credential-ml` does not fit in a request of `aws-comprehend` or
`aws-bedrock-guardrails`. With one of them, `cp` reads a text in pieces only when the local redacter comes first in the
chain, and reads it whole otherwise, so a text over the request limit of the provider then fails. `scan` also reads it whole, but sends it to the provider in parts that fit a request, so it succeeds.
The container of the `presidio-analyzer` image answers one request at a time, and `aws-bedrock-guardrails` is limited in
some Regions to 75 text units per second, against the 100 text units of a full request, so the pieces of both are read
one after another.

A CSV file whose cells take more than about 4 MiB of memory is redacted in batches of rows when every selected redacter is `local-rules`,
`local-credential-ml`, `local-ner` or `local-gliner`, and each batch is written to the destination as it is finished.
A batch holds about 4 MiB of cells, each counted with the memory it takes beside its text, and a row is never cut.
The output is the same as when the file is read whole. A batch is placed as a table of its own, and every
redaction is located in its cell. Other redacters
read the file whole.

A file that is not valid UTF-8 fails with the byte it fails at, also when that byte is far into the file. The file is
not written to a local destination and is left out of a zip archive, see [Source and destinations](../README.md#source-and-destinations).

A file that `--unsupported-as-text` reads as text is redacted in pieces in the same way, so `.jsonl`, `.env`, logs with
other extensions and source code take the same memory as a `.log`. `cp -d local-rules` on a 150 MB `.dat` log with
10 900 findings peaks near 81 MiB of memory, against 639 MiB when it is read whole, in about 4% more time. A file with a
NUL byte or bytes that are not UTF-8 in its first window, the first 4 MiB or so, is skipped without
`--allow-unsupported-copies` and copied as it is with it, as when it is read whole. One that turns out not to be text
later fails with the byte it fails at, and nothing of it is left at the destination, where a whole read skips it
without `--allow-unsupported-copies`. `scan` reports the first as unsupported and the second as failed.

## Scan

A text file of more than about 4 MiB is read in pieces when every selected redacter is `local-rules` or
`local-credential-ml`. This covers plain text and logs, JSON, YAML, XML, HTML, CSS and Markdown. A piece is read with
64 KiB of text on each side, in windows of at most 4 MiB. A value of up to 128 KiB
across lines, such as a private key block, is found wherever a piece ends, and a longer one that crosses the end of a
piece can be missed. A finding is reported once, at the location it has when the file is read whole, except that a
value longer than 64 KiB that crosses the end of a window can also be reported in part. Findings are sorted by location
within a window, so such a value is listed after the findings of the window before it that start later in the file. A line longer than a
window is cut inside, so the text sets a few MiB of the memory `scan` needs, however long its lines are.
`aws-comprehend`, `presidio`, `gcp-dlp` and `aws-bedrock-guardrails` read a text in pieces when it does not fit in one
request, with the limits listed under [Copy](#copy). The LLM redacters, `local-ner` and `local-gliner` read the file whole. A CSV file whose cells take more than about 4 MiB of memory is read in batches of rows
when every selected redacter is `local-rules`, `local-credential-ml`, `local-ner` or `local-gliner`, with the findings and
locations of a whole read. With `--sampling-size`, `scan` reads only as much of a text
file as the sample needs. `scan -d local-rules` on a 329 MB log with 27 findings peaks near 66 MB of memory, against
755 MB in 0.21, in about 2% more time.

The findings of a scan are kept in a temporary file instead of in memory. The file is in the system temporary
directory, has no name, is removed when the scan ends, however it ends, and on Unix is readable by the current user only.
It holds values masked, or as found with `--show-values`, and takes about 200 bytes of disk for each finding. A scan that
cannot write to it stops with an error that names the directory.

`scan -d local-rules --report-file r.json` on an 88 MB log with 2.4 million findings peaks near 910 MB on macOS, where
0.21 peaks near 1.6 GB on the same log. The scan takes about 15% more time than in 0.21. The SARIF report still lists at most 5000 results, and building it keeps at most that many in memory. Building the
OCSF report keeps an event for each file.
