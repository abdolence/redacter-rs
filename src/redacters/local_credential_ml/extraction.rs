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
/// and brackets that wrap a value in code and config, and the backslash that escapes a quote
/// or continues a string literal on the next line.
const WRAPPING_CHARACTERS: &[char] =
    &['"', '\'', '`', ',', ';', '(', ')', '{', '}', '[', ']', '\\'];

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
        for line_range in line_ranges(text) {
            let line = &text[line_range.clone()];
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
    /// with their value checks off, and every credential-like word that holds no such match.
    /// Both, because a rule can match a harmless value (`token: none`) beside the password it
    /// did not recognise, and a redacter prefers redacting a host name beside the password to
    /// leaving the password. A word that holds a match is left to the rule, which places the
    /// value more narrowly than the word does (`//registry.npmjs.org/:_authToken=npm_...`).
    fn values_in_line(&self, line: &str) -> Vec<Range<usize>> {
        let matched: Vec<Range<usize>> = self
            .line_rules
            .find(line)
            .into_iter()
            .map(|finding| finding.start..finding.end)
            .filter(|value| line[value.clone()].chars().any(char::is_alphanumeric))
            .collect();
        let unmatched_words = credential_like_words(line).into_iter().filter(|word| {
            !matched
                .iter()
                .any(|value| value.start < word.end && word.start < value.end)
        });
        matched.iter().cloned().chain(unmatched_words).collect()
    }
}

/// The values of the whitespace-separated words of `line` that hold a letter or digit and a
/// digit or a credential symbol, without the quotes and brackets around them and without the
/// `key=` in front of them when they are `key=value` pairs.
///
/// What is not a value even when it holds such a character: the name an assignment or
/// declaration gives (`API_KEY2 =`, `ED25519_PEM:`, `"key1":`), a Rust reference or type
/// (`&str`, `&[u8`, `Vec<u8>`, the `32` of `[u8; 32]`), a `#directive`, and the code in front of
/// a string literal (`String::from(`, `r#`). Inside a double-quoted literal the value is the
/// literal.
///
/// A word is split at each redaction marker an earlier redacter placed, and the marker is
/// never a candidate: cutting it would leave a stray bracket. What stands beside it
/// (`deploy@[REDACTED]:Tr0ub4dor&3`) still can be, apart from the key of an assignment
/// whose value is the marker (`api_key=[REDACTED]`).
fn credential_like_words(line: &str) -> Vec<Range<usize>> {
    let words: Vec<Range<usize>> = words(line).collect();
    let mut values = Vec::new();
    for (index, word) in words.iter().enumerate() {
        let follows_semicolon = index > 0 && line[words[index - 1].clone()].ends_with(';');
        for piece in pieces_around_markers(line, word.clone()) {
            for segment in literal_segments(line, piece) {
                let unwrapped = trim_wrapping(line, segment.range.clone());
                if !segment.quoted
                    && (is_declared_name(line, &unwrapped)
                        || is_type_or_directive(&line[unwrapped.clone()])
                        || (follows_semicolon && is_array_length(line, word)))
                {
                    continue;
                }
                let value = value_of_word(line, segment.range);
                let text = &line[value.clone()];
                if text.chars().any(char::is_alphanumeric) && quoted_credential_value(text) {
                    values.push(value);
                }
            }
        }
    }
    values
}

/// A part of a word that lies inside or outside double quotes.
struct Segment {
    range: Range<usize>,
    quoted: bool,
}

/// The parts of `piece` between its double quotes. Dropped are the code directly in front of
/// an opening quote (`String::from(`, `key=`, `r#`) and a quoted JSON key (`"kid":`). Whether a
/// quote opens or closes a literal is read from the text next to it and not counted, since the
/// quotes of a raw string or of JSON inside a string are nested.
fn literal_segments(line: &str, piece: Range<usize>) -> Vec<Segment> {
    const CODE_BEFORE_LITERAL: &[char] = &['(', '=', ':', ',', '[', '{', '#'];
    let text = &line[piece.clone()];
    let mut cuts = Vec::new();
    let mut previous = ' ';
    for (offset, character) in text.char_indices() {
        if character == '"' && previous != '\\' {
            cuts.push(piece.start + offset);
        }
        previous = character;
    }
    let mut segments = Vec::new();
    let mut start = piece.start;
    for end in cuts.into_iter().chain([piece.end]) {
        let range = start..end;
        start = end + 1;
        if range.is_empty() {
            continue;
        }
        let quoted = range.start > 0 && line[..range.start].ends_with('"');
        let range = if quoted {
            without_raw_string_terminator(line, range)
        } else {
            range
        };
        let followed_by_quote = line[range.end..].starts_with('"');
        let is_key =
            quoted && followed_by_quote && line[range.end + 1..].trim_start().starts_with(':');
        let is_code =
            !quoted && followed_by_quote && line[range.clone()].ends_with(CODE_BEFORE_LITERAL);
        if !is_key && !is_code {
            segments.push(Segment { range, quoted });
        }
    }
    segments
}

/// `range` without the `#`s that close a raw string (`"#.into()`), told from a value that starts
/// with a `#` by the punctuation that follows them.
fn without_raw_string_terminator(line: &str, range: Range<usize>) -> Range<usize> {
    let text = &line[range.clone()];
    let rest = text.trim_start_matches('#');
    let closes_raw_string = rest.len() < text.len()
        && rest
            .chars()
            .next()
            .is_none_or(|c| matches!(c, '.' | ',' | ';' | ')' | ']' | '}'));
    if closes_raw_string {
        range.start + text.len() - rest.len()..range.end
    } else {
        range
    }
}

/// Whether `name` is an identifier: the name of a variable, field or key.
fn is_identifier(name: &str) -> bool {
    name.chars()
        .next()
        .is_some_and(|first| first.is_alphabetic() || first == '_')
        && name.chars().all(is_identifier_character)
}

fn is_identifier_character(character: char) -> bool {
    character.is_alphanumeric() || matches!(character, '_' | '-' | '.')
}

/// An identifier that `unwrapped` gives a value to: `NAME:` and `NAME:=` end in the separator,
/// and `NAME = value` has it as the next word. A `NAME=` with no space is not matched, since
/// `Zm9vYg=` is base64 padding.
fn is_declared_name(line: &str, unwrapped: &Range<usize>) -> bool {
    let text = &line[unwrapped.clone()];
    if let Some(name) = text.strip_suffix(":=").or_else(|| text.strip_suffix(':')) {
        return is_identifier(name);
    }
    let after = line[unwrapped.end..].trim_start();
    is_identifier(text) && (after.starts_with('=') || after.starts_with(":="))
}

/// A word that is Rust type syntax (`&str`, `&mut`, `&'a`, `&[u8`, `Vec<u8>`, `u8`, or any of
/// them after the `(` of a parameter list) or a
/// preprocessor directive or tag (`#define`). The `&` of a reference and the digits of a
/// primitive type would otherwise pass for credential symbols. A type word holds nothing but
/// identifiers, primitive numeric types and type punctuation, so a word with any other digit
/// (`&x9Lq2`) is not one.
fn is_type_or_directive(text: &str) -> bool {
    const PRIMITIVES: &[&str] = &[
        "u8", "u16", "u32", "u64", "u128", "i8", "i16", "i32", "i64", "i128", "f32", "f64",
    ];
    const TYPE_PUNCTUATION: &str = "[]<>:;,'()";
    if let Some(directive) = text.strip_prefix('#') {
        return !directive.is_empty() && directive.chars().all(|c| c.is_alphabetic() || c == '_');
    }
    let type_text = text
        .rsplit('(')
        .next()
        .unwrap_or(text)
        .trim_start_matches('&');
    type_text
        .chars()
        .all(|c| c.is_alphanumeric() || c == '_' || TYPE_PUNCTUATION.contains(c))
        && type_text
            .split(|c: char| !(c.is_alphanumeric() || c == '_'))
            .all(|token| {
                token.chars().all(|c| c.is_alphabetic() || c == '_') || PRIMITIVES.contains(&token)
            })
}

/// The length in the `32]` of an array type or expression (`[u8; 32]`).
fn is_array_length(line: &str, word: &Range<usize>) -> bool {
    line[word.clone()]
        .trim_end_matches([',', ')', ';'])
        .strip_suffix(']')
        .is_some_and(|length| !length.is_empty() && length.chars().all(|c| c.is_ascii_digit()))
}

/// The parts of `word` between its redaction markers. A part that is the key of an
/// assignment whose value is the marker that follows it is left out.
fn pieces_around_markers(line: &str, word: Range<usize>) -> Vec<Range<usize>> {
    let text = &line[word.clone()];
    let mut pieces = Vec::new();
    let mut start = 0;
    for (marker, _) in text.match_indices(REDACTED) {
        let piece = text[start..marker].trim_end_matches('=');
        let is_assignment_key = piece.len() < marker - start
            && piece
                .chars()
                .all(|c| c.is_alphanumeric() || matches!(c, '_' | '-' | '.'));
        if !is_assignment_key {
            pieces.push(word.start + start..word.start + marker);
        }
        start = marker + REDACTED.len();
    }
    pieces.push(word.start + start..word.end);
    pieces
}

/// The byte ranges of the lines of `text` without their terminators: `\n`, `\r\n` or a bare
/// `\r`.
fn line_ranges(text: &str) -> impl Iterator<Item = Range<usize>> + '_ {
    let mut start = 0;
    std::iter::from_fn(move || {
        if start >= text.len() {
            return None;
        }
        let rest = &text[start..];
        let length = rest.find(['\r', '\n']).unwrap_or(rest.len());
        let terminator = if rest[length..].starts_with("\r\n") {
            2
        } else {
            usize::from(length < rest.len())
        };
        let range = start..start + length;
        start += length + terminator;
        Some(range)
    })
}

/// The byte ranges of the whitespace-separated words of `line`.
fn words(line: &str) -> impl Iterator<Item = Range<usize>> + '_ {
    line.split_whitespace().map(|word| {
        let start = word.as_ptr() as usize - line.as_ptr() as usize;
        start..start + word.len()
    })
}

/// The part of `word` that is the value: without the wrapping characters, a trailing full
/// stop, and the `key=` of a `key=value` pair, whose own quotes are dropped too. The key is the
/// identifier before the `=`, and a call's opening bracket in front of it (`connect(`) goes as
/// well. A word that is only padding after the `=` (`Zm9v==`) is not a pair.
fn value_of_word(line: &str, word: Range<usize>) -> Range<usize> {
    let unwrapped = trim_wrapping(line, word);
    let text = &line[unwrapped.clone()];
    match text.split_once('=') {
        Some((before, rest)) if !rest.trim_start_matches('=').is_empty() => {
            let code_before_key = before.trim_end_matches(is_identifier_character);
            let has_key = code_before_key.len() < before.len();
            if has_key && (code_before_key.is_empty() || code_before_key.ends_with(['(', ','])) {
                trim_wrapping(line, unwrapped.start + before.len() + 1..unwrapped.end)
            } else {
                unwrapped
            }
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
        let mut ranges = extractor().values_in_line(line);
        ranges.sort_by_key(|range| (range.start, range.end));
        ranges.dedup();
        ranges.into_iter().map(|range| &line[range]).collect()
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
    fn a_word_that_holds_a_rule_match_is_not_redacted_whole() {
        assert_eq!(
            values("//registry.npmjs.org/:_authToken=npm_aB3dE5gH7jK9mN1pQ3rS5tU7vW9xY1z3A5c7"),
            ["npm_aB3dE5gH7jK9mN1pQ3rS5tU7vW9xY1z3A5c7"]
        );
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
        assert_eq!(values("password is [REDACTED]"), Vec::<&str>::new());
        assert_eq!(values("api_key=[REDACTED] next"), Vec::<&str>::new());
    }

    #[test]
    fn the_text_around_a_redaction_marker_is_still_searched() {
        assert_eq!(
            values("ssh deploy@[REDACTED] and use the passphrase Tr0ub4dor&3 when asked."),
            ["deploy@", "Tr0ub4dor&3"]
        );
        assert_eq!(
            values("creds: deploy@[REDACTED]:Tr0ub4dor&3"),
            ["deploy@", ":Tr0ub4dor&3"]
        );
    }

    #[test]
    fn a_line_holding_a_marker_is_scored_and_its_password_redacted() {
        let text = "creds: deploy@[REDACTED]:Tr0ub4dor&3";
        let (findings, scored) = find(text, |_| 0.9);
        assert_eq!(scored, [text]);
        assert!(!redact(text, &findings).contains("Tr0ub4dor&3"));
    }

    #[test]
    fn the_quotes_around_an_assigned_value_stay_outside_it() {
        assert_eq!(values(r#"session_blob="Tr0ub4dor&3""#), ["Tr0ub4dor&3"]);
    }

    #[test]
    fn a_word_made_of_symbols_alone_is_not_a_candidate() {
        assert_eq!(values("name = app"), Vec::<&str>::new());
    }

    #[test]
    fn a_reference_type_is_not_a_value() {
        assert_eq!(
            values(r#"const DNI_LETTERS: &[u8] = b"TRWAGMYFPDXBNJZSQVHLCKE";"#),
            Vec::<&str>::new()
        );
        assert_eq!(
            values(r#"const HEADER: &str = "02010a6f72646572732d6575310137";"#),
            ["02010a6f72646572732d6575310137"]
        );
        assert_eq!(
            values(
                "fn open(&mut self, key: &'a [u8; 32], salt: &Vec<u8>) -> Token // Sup3rS3cret!"
            ),
            ["Sup3rS3cret!"]
        );
    }

    #[test]
    fn a_secret_starting_with_an_ampersand_is_still_a_value() {
        assert_eq!(values("PASS=&x9Lq2"), ["&x9Lq2"]);
        assert_eq!(values("PASS=&hunter"), ["&hunter"]);
        assert_eq!(values("password: &x9Lq2 until Friday"), ["&x9Lq2"]);
        assert_eq!(values(r#"password = "&Tr0ub4dor3""#), ["&Tr0ub4dor3"]);
    }

    #[test]
    fn the_name_being_declared_is_not_a_value() {
        assert_eq!(
            values(r#"const ED25519_PKCS8_PEM: &str = "-----BEGIN PRIVATE KEY-----"#),
            Vec::<&str>::new()
        );
        assert_eq!(values("sha256: 9f86d081884c7d65"), ["9f86d081884c7d65"]);
        assert_eq!(values(r#"API_KEY2 = "k8Jd2Lw9""#), ["k8Jd2Lw9"]);
        assert_eq!(values(r#"token1 := "x9Lq2Wm""#), ["x9Lq2Wm"]);
    }

    #[test]
    fn a_json_key_beside_its_value_is_not_a_value() {
        assert_eq!(
            values(r#"{"prime1":"abc","x":"11qYAYKxCrfVS_7T","use":"sig"}"#),
            ["11qYAYKxCrfVS_7T"]
        );
        assert_eq!(values(r#""key1" : "k8Jd2Lw9""#), ["k8Jd2Lw9"]);
    }

    #[test]
    fn the_syntax_around_a_string_literal_is_not_a_value() {
        assert_eq!(
            values(r##"r#"{{"kid": "{URL}", "value": "d3JhcHBlZA"}}"#"##),
            ["d3JhcHBlZA"]
        );
        assert_eq!(
            values(r#"hex::decode("ee1b4efa8e3b1f55")"#),
            ["ee1b4efa8e3b1f55"]
        );
    }

    #[test]
    fn a_line_continuation_backslash_is_not_part_of_the_value() {
        assert_eq!(
            values(r#"const KEY: &str = "0460fed4ba255a9d\"#),
            ["0460fed4ba255a9d"]
        );
    }

    #[test]
    fn a_keyword_argument_is_the_value_after_its_name() {
        assert_eq!(
            values("User(name=bob, password=Sup3rS3cret1)"),
            ["Sup3rS3cret1"]
        );
    }

    #[test]
    fn a_secret_with_symbols_before_an_equals_sign_is_one_value() {
        assert_eq!(values("password is P@ss=w0rd"), ["P@ss=w0rd"]);
    }

    #[test]
    fn an_escaped_quote_is_not_part_of_the_value() {
        assert_eq!(values(r#""\"note\": \"Tr0ub4dor&3\"""#), ["Tr0ub4dor&3"]);
    }

    #[test]
    fn the_end_of_a_raw_string_is_not_a_value() {
        assert_eq!(
            values(r##"r#"password = 'k8Jd2Lw9'"#.into(),"##),
            ["k8Jd2Lw9"]
        );
        assert_eq!(values(r##"note = "#x9Lq2Wm""##), ["#x9Lq2Wm"]);
    }

    #[test]
    fn a_walrus_assignment_places_the_value_after_it() {
        assert_eq!(
            values(r#"client_secret := "e48b89ef0fdf5999""#),
            ["e48b89ef0fdf5999"]
        );
    }

    #[test]
    fn a_preprocessor_directive_is_not_a_value() {
        assert_eq!(values("#define MAX_RETRIES 5"), ["5"]);
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
    fn a_password_beside_a_harmless_rule_match_is_redacted_too() {
        for (text, redacted) in [
            (
                "token: none, use the passphrase Tr0ub4dor&3 instead",
                "token: [REDACTED], use the passphrase [REDACTED] instead",
            ),
            (
                "db_password=changeme; admin login root/Tr0ub4dor&3",
                "db_password=[REDACTED]; admin login [REDACTED]",
            ),
        ] {
            let (findings, _) = find(text, |_| 0.9);
            assert_eq!(redact(text, &findings), redacted);
        }
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
    fn a_bare_carriage_return_ends_a_line() {
        let text = "password: Tr0ub4dor&3\rname = app\rtoken: Zx9!Qw8@Er7\r";
        let (findings, scored) = find(
            text,
            |line| if line.starts_with("name") { 0.0 } else { 0.9 },
        );
        assert_eq!(
            redact(text, &findings),
            "password: [REDACTED]\rname = app\rtoken: [REDACTED]\r"
        );
        assert_eq!(scored, ["password: Tr0ub4dor&3", "token: Zx9!Qw8@Er7"]);
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
