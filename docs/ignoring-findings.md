# Ignoring files and findings

Two mechanisms keep legitimate values, such as the mock keys of a test fixture, out of a scan. Both apply to `scan`
only. `cp` does not read an ignore file and does not look for the markers: it always redacts everything it finds.

Be aware that both hide real secrets as well as mock ones, so keep the ignore file, the marked lines and the marked regions in code review.

## Ignore file

An ignore file lists the files `scan` does not read. It uses the gitignore syntax: `#` comments, `*` and `**`
wildcards, a trailing `/` for a directory, a leading `/` to anchor a pattern at the source root and `!` to bring back a
file an earlier pattern ignored. Unlike git, `!` also brings back a file inside an ignored directory: with `fixtures/`
and `!fixtures/keep.txt`, `keep.txt` is scanned. Patterns match the path of a file relative to the source root, the path
the reports show, with `/` or `\` as the separator, so the same file works for a local directory, GCS, S3 and a zip
archive.

```
# Mock credentials used by the tests
test-fixtures/
*.mock.txt
docs/**/example-keys.md
!docs/real/example-keys.md
```

- A local directory source uses the `.redacterignore` at its root when it has one. The file leaves itself out, so it is
  neither scanned nor reported as unsupported, and it is counted in `files_ignored`;
- `--ignore-file <PATH>` reads the given local file instead, for any source, including GCS and S3;
- `--no-ignore` skips the `.redacterignore` of a local source. It cannot be combined with `--ignore-file`, and the
  markers below still apply;
- a file named as the source itself is always scanned;
- `.git` directories are left out of a local directory source whatever the ignore settings, see
  [Source and destinations](../README.md#source-and-destinations);
- ignored files do not count towards `--max-files-limit`;
- a UTF-8 byte order mark at the start of the file is dropped, as git does;
- a pattern that cannot be compiled is an error that names the file and the line, and the command exits with 1.

An ignored file is not read. It is counted in `files_ignored`, which is part of the file total of the summary, and has no
entry in the `files` of the report. The other counters keep their meaning.

## Inline marker

The text `redacter:allow` on a line ignores every finding that covers that line, from the line where the finding
starts to the line where it ends. The marker is matched as a substring, so it sits in whatever comment syntax the file
uses:

```
API_KEY=AKIAIOSFODNN7EXAMPLE # redacter:allow
```

A private key block spans several lines, so a marker on any of them, such as the `END` line, ignores the whole key:

```
-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEA...
-----END RSA PRIVATE KEY----- # redacter:allow
```

A marker inside the value of any finding does not count, and ignores nothing: `API_KEY=xyz_redacter:allow_123` is still
reported, and so is an email on the same line. A marker on a line the finding does not cover, such as the line above it,
does not count either.

- The ignored findings are not in `findings`, in the rows or in the exit code. They are counted in `findings_ignored`,
  so the suppression stays visible in the summary and in the JSON report;
- the marker works for every redacter, since it acts on the location of a finding. A value an LLM redacter named
  without being able to locate it has no line and is not ignored; it stays in `unlocated_values`;
- in a CSV table the marker has to be inside the cell of the finding. A CSV file read as plain text follows the rule of
  lines;
- on a line longer than about 4 MiB, in a file read in pieces, the marker is not read;
- `redacter:allow-begin` and `redacter:allow-end` are not inline markers, see [Block markers](#block-markers);
- an image or a PDF page has no place for the marker, so the findings in them are always reported;
- `--min-score` is applied first, so `findings_ignored` counts only the findings that would have been reported.

## Block markers

`redacter:allow-begin` and `redacter:allow-end` ignore every finding on the lines between them. The inline marker has to
stand on a line of the finding. A region is for a value that has no such line to spare, such as a key in a Markdown code
block, where a marker would show in the rendered page, and for a place that holds several values, such as a fixture,
where a marker on each line is noise.

In Markdown the markers sit in HTML comments:

````markdown
<!-- redacter:allow-begin -->
```text
-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIJ+...
-----END PRIVATE KEY-----
```
<!-- redacter:allow-end -->
````

In source code they sit in a comment above and below the value:

```rust
// redacter:allow-begin
const TEST_KEY: &str = "-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIJ+...
-----END PRIVATE KEY-----";
// redacter:allow-end
```

Like the inline marker, they are matched as substrings, in whatever comment syntax the file uses.

Be aware that a region ignores real secrets added inside it later, and nothing reports them. Keep regions small, around
the one value they are for, and review a change that touches one.

On a marker line, only the inline marker ignores a finding. Here `a@b.io` is reported and `c@d.io` is ignored, and in the
second example both are ignored, since the inline marker stands beside the begin:

```
a@b.io # redacter:allow-begin
c@d.io
# redacter:allow-end

a@b.io redacter:allow redacter:allow-begin
c@d.io
# redacter:allow-end
```

The markers of a line are read in the order they stand, so an end before a begin closes a region and opens the next one:

```
# redacter:allow-begin
first value
# redacter:allow-end redacter:allow-begin
second value
# redacter:allow-end
```

- A region is the lines between the begin line and the end line, not including them. A finding is ignored when it lies
  entirely in a region. One that starts on the begin line, or ends on the end line, is reported;
- a marker inside the value of a finding does not count, as for the inline marker. With `--categories`, a value of a
  category that is not selected is not a finding, so a marker inside it counts, for both forms of the marker;
- the ignored findings are counted in `findings_ignored`, as for the inline marker. `--min-score` is applied first;
- block markers apply to text. A CSV table read by cell has no region, an image or a PDF page has no place for the
  markers, and `cp` redacts everything.

A marker that does not pair up gives a warning with the line of the marker, and never hides a finding:

- a begin with no end after it has no effect, so a forgotten end marker does not hide the rest of the file;
- an end with no begin before it has no effect;
- a begin inside a region that is already open has no effect, and the region keeps its first begin.

The warnings are listed in the order of the file. They are shown under the row of the file in the terminal and in the
text report, and are the `marker_warnings` of the file in the JSON report. In a SARIF report each is a warning in the
notifications of the run, on the line of the marker. OCSF reports do not carry them. The warnings do not change the exit
code. With `--sampling-size`, a begin with no end in the sample gets no warning, as the sample can end inside a region.

A file larger than one request is read in pieces: about 3.9 MiB for `local-rules` and `local-credential-ml`, and the
request size of the provider less twice its context for the cloud providers. When several redacters are selected, the
pieces fit all of them. Block markers are read on a line of any length, and the region state is carried from one piece to
the next, so a region can start in an earlier piece than a finding. Two things differ from a whole read:

- a finding is ignored only when its end marker lies before the last context of its piece (the last 64 KiB with
  `local-rules`). A finding with its end marker in that context, or in a later piece, is reported;
- a marker inside a value of up to twice the context (128 KiB with `local-rules`) never counts, wherever the pieces
  are cut. A longer value that crosses the end of a piece can be missed, as for any finding, and then
  a marker inside it counts.
