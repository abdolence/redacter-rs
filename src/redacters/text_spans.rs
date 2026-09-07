use std::fmt::Display;

pub const REDACTED: &str = "[REDACTED]";

/// Name of the rule that produced a finding, e.g. `email` or a user's `employee-id`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RuleName(String);

impl RuleName {
    pub fn new(name: &str) -> Self {
        Self(name.to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for RuleName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// A byte range of the input text matched by one rule. `start..end` is half-open.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Finding {
    pub start: usize,
    pub end: usize,
    pub rule: RuleName,
}

/// Sorts findings by start and drops any finding that overlaps an earlier kept one. On the
/// same start the longest wins; on identical spans the first in input order wins, which is
/// the rule declaration order.
pub fn merge_findings(mut findings: Vec<Finding>) -> Vec<Finding> {
    findings.sort_by(|a, b| a.start.cmp(&b.start).then(b.end.cmp(&a.end)));
    let mut merged: Vec<Finding> = Vec::with_capacity(findings.len());
    for finding in findings {
        match merged.last() {
            Some(last) if finding.start < last.end => continue,
            _ => merged.push(finding),
        }
    }
    merged
}

/// Rebuilds `text` with every finding replaced by `replacement`. Findings must be merged
/// (sorted, non-overlapping); the function walks them front to back.
pub fn apply_redaction(text: &str, findings: &[Finding], replacement: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut cursor = 0;
    for finding in findings {
        out.push_str(&text[cursor..finding.start]);
        out.push_str(replacement);
        cursor = finding.end;
    }
    out.push_str(&text[cursor..]);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn finding(start: usize, end: usize, rule: &str) -> Finding {
        Finding {
            start,
            end,
            rule: RuleName::new(rule),
        }
    }

    #[test]
    fn merge_sorts_by_start() {
        let merged = merge_findings(vec![finding(10, 12, "b"), finding(0, 3, "a")]);
        assert_eq!(merged, vec![finding(0, 3, "a"), finding(10, 12, "b")]);
    }

    #[test]
    fn merge_keeps_the_longest_of_two_overlapping_findings() {
        let merged = merge_findings(vec![finding(0, 5, "short"), finding(0, 9, "long")]);
        assert_eq!(merged, vec![finding(0, 9, "long")]);
    }

    #[test]
    fn merge_drops_a_finding_starting_inside_an_earlier_one() {
        let merged = merge_findings(vec![finding(0, 9, "a"), finding(4, 12, "b")]);
        assert_eq!(merged, vec![finding(0, 9, "a")]);
    }

    #[test]
    fn merge_prefers_the_earlier_declared_rule_on_identical_spans() {
        let merged = merge_findings(vec![finding(2, 6, "first"), finding(2, 6, "second")]);
        assert_eq!(merged, vec![finding(2, 6, "first")]);
    }

    #[test]
    fn merge_keeps_adjacent_findings() {
        let merged = merge_findings(vec![finding(0, 3, "a"), finding(3, 6, "b")]);
        assert_eq!(merged.len(), 2);
    }

    #[test]
    fn apply_replaces_each_finding_with_the_token() {
        let text = "mail a@b.io or call 123";
        let findings = vec![finding(5, 11, "email"), finding(20, 23, "phone")];
        assert_eq!(
            apply_redaction(text, &findings, REDACTED),
            "mail [REDACTED] or call [REDACTED]"
        );
    }

    #[test]
    fn apply_handles_multibyte_text_before_a_finding() {
        let text = "héllo wörld a@b.io";
        let start = text.find("a@b.io").unwrap();
        let findings = vec![finding(start, start + 6, "email")];
        assert_eq!(
            apply_redaction(text, &findings, REDACTED),
            "héllo wörld [REDACTED]"
        );
    }

    #[test]
    fn apply_with_no_findings_returns_the_text_unchanged() {
        assert_eq!(
            apply_redaction("nothing here", &[], REDACTED),
            "nothing here"
        );
    }
}
