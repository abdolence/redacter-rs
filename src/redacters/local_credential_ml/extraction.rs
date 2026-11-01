//! Turns the model's verdict on a line into the byte ranges to redact. The classifier says
//! that a line holds a credential and nothing about where, so the ranges come from the
//! `local-rules` secret rules and, failing those, from every credential-like word of the line.

use super::error::LocalCredentialMlError;
use crate::redacters::quoted_credential_value;
use crate::redacters::text_spans::{merge_findings, Finding, RuleName, REDACTED};
use crate::redacters::{RuleSet, Score};
use std::ops::Range;

/// The label of every finding, whichever rule or heuristic placed it.
const CREDENTIAL_LABEL: &str = "credential";

/// Characters a word is trimmed of at both ends before it is considered a value: the quotes
/// and brackets that wrap a value in code and config.
const WRAPPING_CHARACTERS: &[char] = &['"', '\'', '`', ',', ';', '(', ')', '{', '}', '[', ']'];

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

    /// The credentials in `text`: every private key block, and for every line `score_line`
    /// rates at `min_score` or more, the values on that line, labelled `credential` and
    /// carrying the line's score.
    ///
    /// A private key block is certain on its own, so it is redacted whatever the model
    /// thinks, and the lines inside it are not scored. A line with no value to place is not
    /// scored either, since the model's verdict on it could not change the result.
    pub fn find(
        &self,
        text: &str,
        min_score: Score,
        mut score_line: impl FnMut(&str) -> Result<Score, LocalCredentialMlError>,
    ) -> Result<Vec<Finding>, LocalCredentialMlError> {
        let blocks = self.private_key_blocks.find(text);
        let mut findings: Vec<Finding> = blocks
            .iter()
            .map(|block| Finding {
                start: block.start,
                end: block.end,
                rule: RuleName::new(CREDENTIAL_LABEL),
                score: None,
            })
            .collect();
        let mut line_start = 0;
        for raw_line in text.split_inclusive('\n') {
            let line = raw_line.trim_end_matches(['\r', '\n']);
            let line_range = line_start..line_start + line.len();
            line_start += raw_line.len();
            let inside_block = blocks
                .iter()
                .any(|block| block.start <= line_range.start && line_range.end <= block.end);
            if inside_block {
                continue;
            }
            let values = self.values_in_line(line);
            if values.is_empty() {
                continue;
            }
            let score = score_line(line)?;
            if score < min_score {
                continue;
            }
            findings.extend(values.into_iter().map(|range| Finding {
                start: line_range.start + range.start,
                end: line_range.start + range.end,
                rule: RuleName::new(CREDENTIAL_LABEL),
                score: Some(score),
            }));
        }
        Ok(merge_findings(findings))
    }

    /// The values to redact on a line the model flagged: what the secret rules match on it
    /// with their value checks off, or else every credential-like word. A redacter prefers
    /// redacting a host name beside the password to leaving the password.
    fn values_in_line(&self, line: &str) -> Vec<Range<usize>> {
        let matched = self.line_rules.find(line);
        if matched.is_empty() {
            return credential_like_words(line);
        }
        matched
            .into_iter()
            .map(|finding| finding.start..finding.end)
            .collect()
    }
}

/// The values of the whitespace-separated words of `line` that hold a letter or digit and a
/// digit or a credential symbol, without the quotes and brackets around them and without the
/// `key=` in front of them when they are `key=value` pairs. A word that holds a redaction
/// marker is left alone: an earlier redacter placed it, and cutting it again would leave a
/// stray bracket.
fn credential_like_words(line: &str) -> Vec<Range<usize>> {
    words(line)
        .filter(|word| !line[word.clone()].contains(REDACTED))
        .map(|word| value_of_word(line, word))
        .filter(|value| {
            let text = &line[value.clone()];
            text.chars().any(char::is_alphanumeric) && quoted_credential_value(text)
        })
        .collect()
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
/// stop, and the `key=` of a `key=value` pair, whose own quotes are dropped too. A word that
/// is only padding after the `=` (`Zm9v==`) is not a pair.
fn value_of_word(line: &str, word: Range<usize>) -> Range<usize> {
    let unwrapped = trim_wrapping(line, word);
    let text = &line[unwrapped.clone()];
    match text.split_once('=') {
        Some((key, rest))
            if !key.is_empty()
                && key
                    .chars()
                    .all(|c| c.is_alphanumeric() || matches!(c, '_' | '-' | '.'))
                && !rest.trim_start_matches('=').is_empty() =>
        {
            trim_wrapping(line, unwrapped.start + key.len() + 1..unwrapped.end)
        }
        _ => unwrapped,
    }
}

/// `range` of `line` without the wrapping characters at both ends and a trailing full stop.
fn trim_wrapping(line: &str, range: Range<usize>) -> Range<usize> {
    let text = &line[range.clone()];
    let without_start = text.trim_start_matches(WRAPPING_CHARACTERS);
    let start = range.start + text.len() - without_start.len();
    let without_end = without_start.trim_end_matches(|character: char| {
        character == '.' || WRAPPING_CHARACTERS.contains(&character)
    });
    start..start + without_end.len()
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn every_credential_like_word_on_a_flagged_line_is_redacted() {
        assert_eq!(
            values("machine 10.0.0.5 login admin password hunter2x"),
            ["10.0.0.5", "hunter2x"]
        );
        assert_eq!(
            values("Ping me at 14:30 about build 4512, the password is hunter2x until Friday."),
            ["14:30", "4512", "hunter2x"]
        );
        assert_eq!(
            values("ssh deploy@10.20.30.40 and use the passphrase Tr0ub4dor&3 when asked."),
            ["deploy@10.20.30.40", "Tr0ub4dor&3"]
        );
    }

    #[test]
    fn a_redaction_marker_is_never_a_candidate() {
        assert_eq!(
            values("ssh deploy@[REDACTED] and use the passphrase Tr0ub4dor&3 when asked."),
            ["Tr0ub4dor&3"]
        );
        assert_eq!(values("password is [REDACTED]"), Vec::<&str>::new());
    }

    #[test]
    fn the_quotes_around_an_assigned_value_stay_outside_it() {
        assert_eq!(values(r#"session_blob="Tr0ub4dor&3""#), ["Tr0ub4dor&3"]);
        assert_eq!(values("session_blob='Tr0ub4dor&3'"), ["Tr0ub4dor&3"]);
    }

    #[test]
    fn a_short_quoted_value_is_found_on_a_flagged_line() {
        assert_eq!(values(r#"password = "hunter""#), ["hunter"]);
        assert_eq!(values("password = 'hunter2'"), ["hunter2"]);
    }

    #[test]
    fn a_word_made_of_symbols_alone_is_not_a_candidate() {
        assert_eq!(values("name = app"), Vec::<&str>::new());
    }

    fn find(text: &str, score_of: impl Fn(&str) -> f32) -> (Vec<Finding>, Vec<String>) {
        let mut scored = Vec::new();
        let findings = extractor()
            .find(text, Score::from_static(0.74), |line| {
                scored.push(line.to_string());
                Ok::<_, LocalCredentialMlError>(Score::from_static(score_of(line)))
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
    fn lines_with_nothing_to_place_never_reach_the_model() {
        let (_, scored) = find("just words\n\n   \npassword = Sup3r!\n", |_| 0.0);
        assert_eq!(scored, ["password = Sup3r!"]);
    }

    const PRIVATE_KEY: &str = "-----BEGIN RSA PRIVATE KEY-----\nMIIBOgIBAAJBAKj34GkxFhD90vcNLYLInFEX6Ppy1tPf9Cnzj4p4WGeKLs1Pt8Qu\nKUpRKfFLfRYC9AIKjbJTWit+CqvjWYzvQwECAwEAAQ==\n-----END RSA PRIVATE KEY-----";

    #[test]
    fn a_private_key_is_redacted_whatever_the_model_scores() {
        let text = format!("key follows\n{PRIVATE_KEY}\nafter\n");
        let (findings, scored) = find(&text, |_| 0.0);
        assert_eq!(redact(&text, &findings), "key follows\n[REDACTED]\nafter\n");
        assert_eq!(scored, Vec::<String>::new());
    }
}
