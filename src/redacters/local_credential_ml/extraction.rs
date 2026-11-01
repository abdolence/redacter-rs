//! Turns the model's verdict on a line into the byte ranges to redact. The classifier says
//! that a line holds a credential and nothing about where, so the ranges come from the
//! `local-rules` secret rules and, failing those, from the line's most credential-like word.

use super::error::LocalCredentialMlError;
use crate::redacters::text_spans::{merge_findings, Finding, RuleName};
use crate::redacters::{RuleSet, Score};
use std::ops::Range;

/// The label of every finding, whichever rule or heuristic placed it.
const CREDENTIAL_LABEL: &str = "credential";

/// Characters a word is trimmed of at both ends before it is considered a value: the quotes
/// and brackets that wrap a value in code and config.
const WRAPPING_CHARACTERS: &[char] = &['"', '\'', '`', ',', ';', '(', ')', '{', '}', '[', ']'];

/// Symbols that make a word look like a password or key. Letters alone, and the `-`, `_`, `.`
/// and `/` of ordinary words, hostnames and paths, do not.
const CREDENTIAL_SYMBOLS: &str = "!@#$%^&*+=~?";

#[derive(Debug)]
pub struct CredentialExtractor {
    line_rules: RuleSet,
    private_key_blocks: RuleSet,
}

impl CredentialExtractor {
    pub fn new() -> Result<Self, LocalCredentialMlError> {
        Ok(Self {
            line_rules: RuleSet::credential_line_rules()?,
            private_key_blocks: RuleSet::private_key_block_rule()?,
        })
    }

    /// The credentials in `text`: for every line `score_line` rates at `min_score` or more,
    /// the values on that line, labelled `credential` and carrying the line's score.
    ///
    /// A blank line is never scored. A private key spans lines and the model rates only its
    /// `BEGIN` line, so a flagged `BEGIN` line redacts the whole block, and the lines inside
    /// a block are not scored.
    pub fn find<E>(
        &self,
        text: &str,
        min_score: Score,
        mut score_line: impl FnMut(&str) -> Result<Score, E>,
    ) -> Result<Vec<Finding>, E> {
        let blocks = self.private_key_blocks.find(text);
        let mut findings = Vec::new();
        let mut line_start = 0;
        for raw_line in text.split_inclusive('\n') {
            let line = raw_line.trim_end_matches(['\r', '\n']);
            let line_range = line_start..line_start + line.len();
            line_start += raw_line.len();
            let inside_block = blocks
                .iter()
                .any(|block| block.start < line_range.start && line_range.start < block.end);
            if line.trim().is_empty() || inside_block {
                continue;
            }
            let score = score_line(line)?;
            if score < min_score {
                continue;
            }
            let block_ranges = blocks
                .iter()
                .filter(|block| line_range.contains(&block.start))
                .map(|block| block.start..block.end);
            let line_ranges = self
                .values_in_line(line)
                .into_iter()
                .map(|range| line_range.start + range.start..line_range.start + range.end);
            findings.extend(block_ranges.chain(line_ranges).map(|range| Finding {
                start: range.start,
                end: range.end,
                rule: RuleName::new(CREDENTIAL_LABEL),
                score: Some(score),
            }));
        }
        Ok(merge_findings(findings))
    }

    /// The values to redact on a line the model flagged: what the secret rules match on it
    /// with their value checks off, or else the most credential-like word.
    fn values_in_line(&self, line: &str) -> Vec<Range<usize>> {
        let matched = self.line_rules.find(line);
        if matched.is_empty() {
            return credential_like_word(line).into_iter().collect();
        }
        matched
            .into_iter()
            .map(|finding| finding.start..finding.end)
            .collect()
    }
}

/// The longest whitespace-separated word of `line` that holds a digit or a symbol, without
/// the quotes and brackets around it, and without the `key=` in front of it when it is a
/// `key=value` pair. The first of equally long words wins.
fn credential_like_word(line: &str) -> Option<Range<usize>> {
    let mut longest: Option<(usize, Range<usize>)> = None;
    for word in words(line) {
        let value = value_of_word(line, word);
        let text = &line[value.clone()];
        if !text.chars().any(is_credential_character) {
            continue;
        }
        let length = text.chars().count();
        if longest.as_ref().is_none_or(|(best, _)| length > *best) {
            longest = Some((length, value));
        }
    }
    longest.map(|(_, value)| value)
}

fn is_credential_character(character: char) -> bool {
    character.is_ascii_digit() || CREDENTIAL_SYMBOLS.contains(character)
}

/// The byte ranges of the whitespace-separated words of `line`.
fn words(line: &str) -> impl Iterator<Item = Range<usize>> + '_ {
    let mut characters = line.char_indices().peekable();
    std::iter::from_fn(move || {
        let start = loop {
            let (index, character) = characters.next()?;
            if !character.is_whitespace() {
                break index;
            }
        };
        let mut end = line.len();
        while let Some(&(index, character)) = characters.peek() {
            if character.is_whitespace() {
                end = index;
                break;
            }
            characters.next();
        }
        Some(start..end)
    })
}

/// The part of `word` that is the value: without the wrapping characters, a trailing full
/// stop, and the `key=` of a `key=value` pair. A word that is only padding after the `=`
/// (`Zm9v==`) is not a pair.
fn value_of_word(line: &str, word: Range<usize>) -> Range<usize> {
    let text = &line[word.clone()];
    let unwrapped = text.trim_start_matches(WRAPPING_CHARACTERS);
    let start = word.start + text.len() - unwrapped.len();
    let unwrapped = unwrapped.trim_end_matches(|character: char| {
        character == '.' || WRAPPING_CHARACTERS.contains(&character)
    });
    let value = start..start + unwrapped.len();
    match unwrapped.split_once('=') {
        Some((key, rest))
            if !key.is_empty()
                && key
                    .chars()
                    .all(|c| c.is_alphanumeric() || matches!(c, '_' | '-' | '.'))
                && !rest.trim_start_matches('=').is_empty() =>
        {
            value.start + key.len() + 1..value.end
        }
        _ => value,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::Infallible;

    fn extractor() -> CredentialExtractor {
        CredentialExtractor::new().unwrap()
    }

    /// The text the extractor picks on a line the model flagged.
    fn values(line: &str) -> Vec<&str> {
        extractor()
            .values_in_line(line)
            .into_iter()
            .map(|range| &line[range])
            .collect()
    }

    #[test]
    fn a_password_in_a_sentence_is_the_word_with_digits_and_symbols() {
        assert_eq!(
            values("The password is Tr0ub4dor&3 for the staging box (prose)."),
            ["Tr0ub4dor&3"]
        );
    }

    #[test]
    fn a_secret_after_a_label_no_rule_knows_is_found_by_its_shape() {
        assert_eq!(
            values("Google OAuth: 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"),
            ["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"]
        );
        assert_eq!(
            values("machine example.com login deploy password n3trc-pass"),
            ["n3trc-pass"]
        );
    }

    #[test]
    fn the_key_of_a_key_value_pair_stays() {
        assert_eq!(
            values("Cookie: sessionid=abcdef1234567890abcdef1234567890"),
            ["abcdef1234567890abcdef1234567890"]
        );
    }

    #[test]
    fn base64_padding_is_part_of_the_value_and_not_a_key() {
        assert_eq!(
            values("opaque blob Zm9vYmFyMTIz== from the vendor"),
            ["Zm9vYmFyMTIz=="]
        );
    }

    #[test]
    fn quotes_and_a_closing_full_stop_stay_outside_the_value() {
        assert_eq!(values(r#"auth = "tok3n-value"."#), ["tok3n-value"]);
    }

    #[test]
    fn the_secret_rules_place_the_value_and_leave_the_key() {
        assert_eq!(
            values("export GITHUB_TOKEN=ghp_Zy9Xw8Vu7Ts6Rq5Po4Nm3Lk2Ji1Hg0FeDc9"),
            ["ghp_Zy9Xw8Vu7Ts6Rq5Po4Nm3Lk2Ji1Hg0FeDc9"]
        );
        assert_eq!(
            values("GET /api Authorization: Bearer eyJhbGci.eyJzdWIi.c2lnbmF0dXJl"),
            ["eyJhbGci.eyJzdWIi.c2lnbmF0dXJl"]
        );
        assert_eq!(
            values("DATABASE_URL=postgres://app:pg-pass-23@db.example.com/app"),
            ["app:pg-pass-23"]
        );
    }

    /// The model has already judged the line, so a value the rules would reject on an
    /// unjudged line (short, no digit) is still a value.
    #[test]
    fn the_rules_value_checks_are_off_on_a_flagged_line() {
        assert_eq!(values("password: hunter"), ["hunter"]);
    }

    #[test]
    fn a_line_without_a_credential_like_word_has_nothing_to_redact() {
        assert_eq!(values("the quick brown fox"), Vec::<&str>::new());
    }

    #[test]
    fn the_first_of_equally_long_words_wins() {
        assert_eq!(values("a1b2c3 d4e5f6"), ["a1b2c3"]);
    }

    fn find(text: &str, score_of: impl Fn(&str) -> f32) -> (Vec<Finding>, Vec<String>) {
        let mut scored = Vec::new();
        let findings = extractor()
            .find(text, Score::from_static(0.74), |line| {
                scored.push(line.to_string());
                Ok::<_, Infallible>(Score::from_static(score_of(line)))
            })
            .unwrap();
        (findings, scored)
    }

    fn redact(text: &str, findings: &[Finding]) -> String {
        crate::redacters::text_spans::apply_redaction(
            text,
            findings,
            crate::redacters::text_spans::REDACTED,
        )
    }

    #[test]
    fn only_the_lines_the_model_flags_are_redacted() {
        let text = "name = app\npassword = Sup3rS3cret!\nretries = 3\n";
        let (findings, _) = find(
            text,
            |line| if line.contains("password") { 0.9 } else { 0.1 },
        );
        assert_eq!(
            redact(text, &findings),
            "name = app\npassword = [REDACTED]\nretries = 3\n"
        );
        assert_eq!(findings[0].rule, RuleName::new("credential"));
        assert_eq!(findings[0].score, Some(Score::from_static(0.9)));
    }

    #[test]
    fn a_line_at_the_threshold_is_flagged_and_one_below_it_is_not() {
        let text = "token = abc12345\nsecret = def67890\n";
        let (findings, _) = find(
            text,
            |line| if line.contains("token") { 0.74 } else { 0.73 },
        );
        assert_eq!(
            redact(text, &findings),
            "token = [REDACTED]\nsecret = def67890\n"
        );
    }

    #[test]
    fn offsets_count_bytes_after_multibyte_text_and_windows_line_endings() {
        let text = "пароль\r\npassword: Tr0ub4dor&3\r\nend\r\n";
        let (findings, _) = find(
            text,
            |line| if line.contains("password") { 0.9 } else { 0.0 },
        );
        assert_eq!(
            redact(text, &findings),
            "пароль\r\npassword: [REDACTED]\r\nend\r\n"
        );
    }

    #[test]
    fn blank_lines_never_reach_the_model() {
        let (_, scored) = find("one 1\n\n   \ntwo 2\n", |_| 0.0);
        assert_eq!(scored, ["one 1", "two 2"]);
    }

    const PRIVATE_KEY: &str = "-----BEGIN RSA PRIVATE KEY-----\nMIIBOgIBAAJBAKj34GkxFhD90vcNLYLInFEX6Ppy1tPf9Cnzj4p4WGeKLs1Pt8Qu\nKUpRKfFLfRYC9AIKjbJTWit+CqvjWYzvQwECAwEAAQ==\n-----END RSA PRIVATE KEY-----";

    #[test]
    fn a_flagged_begin_line_redacts_the_whole_private_key() {
        let text = format!("key follows\n{PRIVATE_KEY}\nafter\n");
        let (findings, scored) = find(&text, |line| if line.contains("BEGIN") { 0.9 } else { 0.1 });
        assert_eq!(redact(&text, &findings), "key follows\n[REDACTED]\nafter\n");
        assert_eq!(
            scored,
            ["key follows", "-----BEGIN RSA PRIVATE KEY-----", "after"]
        );
    }

    #[test]
    fn a_private_key_whose_begin_line_is_not_flagged_is_left_alone() {
        let text = format!("{PRIVATE_KEY}\n");
        let (findings, _) = find(&text, |_| 0.1);
        assert_eq!(findings, Vec::new());
    }
}
