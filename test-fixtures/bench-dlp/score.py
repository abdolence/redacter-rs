#!/usr/bin/env python3
"""Score the results produced by run.sh against truth.json.

Reads test-fixtures/bench-dlp/results/<provider>/{run1,run2,run3,perfile,timings.json}
and test-fixtures/bench-dlp/truth.json, then prints a markdown table and writes it
to test-fixtures/bench-dlp/results/summary.md.

truth.json lists, per file, three sets of exact substrings: "pii" (person
names, emails, phones, postal addresses, national ids, card/iban numbers,
dates of birth -- must be gone), "entities" (city/country/organisation names
-- reported, not judged: some providers intentionally redact these, others
don't), and "keep" (non-DOB dates, order/ticket/invoice numbers, amounts,
version strings, generic words -- must survive).

Scoring rule, matching run.sh's outputs: a truth string counts as "gone" (a
pii hit, a redacted entity, or a lost keep string) when it is no longer
present anywhere, as an exact substring, in the redacted file. This is
intentionally coarse -- it does not check that the *right* span was redacted,
only that the original text is gone.
"""
import json
import pathlib
import statistics
import sys

SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
TRUTH_PATH = SCRIPT_DIR / "truth.json"
RESULTS_DIR = SCRIPT_DIR / "results"
REDACTED_TOKEN = "[REDACTED]"


def median_ms(values):
    if not values:
        return None
    return statistics.median(values)


def score_provider(provider_dir: pathlib.Path, truth: dict):
    timings_path = provider_dir / "timings.json"
    if not timings_path.exists():
        return None
    timings = json.loads(timings_path.read_text(encoding="utf-8"))

    whole_ms = timings.get("whole_corpus_ms", [])
    per_file_ms = timings.get("per_file_ms", {})

    pii_total = 0
    pii_hits = 0
    entities_total = 0
    entities_redacted = 0
    keep_total = 0
    keep_survived = 0
    redacted_count = 0
    missing_files = []

    # Use run1's output as the canonical redacted content for correctness scoring.
    run_dir = provider_dir / "run1"
    for fname, spec in truth.items():
        out_path = run_dir / fname
        if not out_path.exists():
            missing_files.append(fname)
            pii_total += len(spec["pii"])
            entities_total += len(spec.get("entities", []))
            keep_total += len(spec["keep"])
            continue
        text = out_path.read_text(encoding="utf-8", errors="replace")
        redacted_count += text.count(REDACTED_TOKEN)

        for s in spec["pii"]:
            pii_total += 1
            if s not in text:
                pii_hits += 1

        for s in spec.get("entities", []):
            entities_total += 1
            if s not in text:
                entities_redacted += 1

        for s in spec["keep"]:
            keep_total += 1
            if s in text:
                keep_survived += 1

    return {
        "provider": provider_dir.name,
        "whole_median_ms": median_ms(whole_ms),
        "whole_runs_ms": whole_ms,
        "per_file_median_ms": median_ms(list(per_file_ms.values())),
        "per_file_ms": per_file_ms,
        "pii_hits": pii_hits,
        "pii_total": pii_total,
        "entities_redacted": entities_redacted,
        "entities_total": entities_total,
        "keep_survived": keep_survived,
        "keep_total": keep_total,
        "redacted_count": redacted_count,
        "missing_files": missing_files,
    }


def fmt_ms(v):
    if v is None:
        return "n/a"
    return f"{v:.0f} ms"


def main():
    if not TRUTH_PATH.exists():
        print(f"error: {TRUTH_PATH} not found", file=sys.stderr)
        return 1
    truth = json.loads(TRUTH_PATH.read_text(encoding="utf-8"))

    if not RESULTS_DIR.exists():
        print(f"error: {RESULTS_DIR} not found -- run ./run.sh first", file=sys.stderr)
        return 1

    provider_dirs = sorted(
        p for p in RESULTS_DIR.iterdir() if p.is_dir() and p.name != "corpus-view"
    )
    if not provider_dirs:
        print(f"error: no provider results under {RESULTS_DIR} -- run ./run.sh first", file=sys.stderr)
        return 1

    rows = []
    for provider_dir in provider_dirs:
        result = score_provider(provider_dir, truth)
        if result is None:
            print(f"warning: skipping {provider_dir.name}, no timings.json (run ./run.sh first)", file=sys.stderr)
            continue
        rows.append(result)

    if not rows:
        print("error: no scoreable results found", file=sys.stderr)
        return 1

    lines = []
    lines.append("| provider | wall time (median of 3) | per-file median | pii hits/total | loc/org redacted/total | keep survived/total | [REDACTED] count |")
    lines.append("|---|---|---|---|---|---|---|")
    for r in rows:
        note = ""
        if r["missing_files"]:
            note = f" (missing: {', '.join(r['missing_files'])})"
        lines.append(
            f"| {r['provider']} | {fmt_ms(r['whole_median_ms'])} | {fmt_ms(r['per_file_median_ms'])} "
            f"| {r['pii_hits']}/{r['pii_total']} | {r['entities_redacted']}/{r['entities_total']} "
            f"| {r['keep_survived']}/{r['keep_total']} "
            f"| {r['redacted_count']}{note} |"
        )

    table = "\n".join(lines)
    print(table)

    summary_path = RESULTS_DIR / "summary.md"
    summary_path.write_text(table + "\n", encoding="utf-8")
    print(f"\nwritten to {summary_path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
