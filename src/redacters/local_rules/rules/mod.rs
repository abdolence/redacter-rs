use super::error::LocalRulesError;
use super::user_rules::{UserMatcher, UserRule};
use super::validators::Validator;
use crate::redacters::text_spans::{apply_redaction, merge_findings, Finding, RuleName, REDACTED};
use clap::ValueEnum;
use regex::{Regex, RegexBuilder};
use std::collections::BTreeSet;
use std::fmt::Display;

mod table;
use table::builtin_rules;

/// Switchable families of built-in rules. `Custom` holds user-defined rules and is always
/// enabled when any are given.
#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum RuleGroup {
    Email,
    Phone,
    PaymentCard,
    Iban,
    Network,
    Url,
    Secrets,
    UsIdentifiers,
    EuIdentifiers,
    WorldIdentifiers,
    Postcodes,
    BirthDates,
    Custom,
}

impl RuleGroup {
    pub fn all() -> BTreeSet<RuleGroup> {
        RuleGroup::value_variants().iter().copied().collect()
    }

    /// One line per group for `--help`-style listings and the rules reference. Read only
    /// by tests and the generator; this is a binary crate, so without the attribute
    /// `-D warnings` fails on "method is never used" (same idiom as `RuleSpec::keyword`).
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn purpose(&self) -> &'static str {
        match self {
            RuleGroup::Email => "Email addresses",
            RuleGroup::Phone => "Phone numbers, international and national forms",
            RuleGroup::PaymentCard => "Payment card numbers",
            RuleGroup::Iban => "International bank account numbers",
            RuleGroup::Network => "IPv4, IPv6 and MAC addresses",
            RuleGroup::Url => "Credentials embedded in URLs",
            RuleGroup::Secrets => "API keys, tokens, authorization headers and private keys",
            RuleGroup::UsIdentifiers => {
                "US identifiers: SSN and ITIN, passports, driver's licenses"
            }
            RuleGroup::EuIdentifiers => {
                "European identifiers: national personal numbers, VAT numbers, UK driving licences"
            }
            RuleGroup::WorldIdentifiers => {
                "Identifiers of Canada, Australia, Brazil, India, South Africa and China"
            }
            RuleGroup::Postcodes => "UK, Irish and Canadian postcodes",
            RuleGroup::BirthDates => "Dates of birth next to a birth keyword",
            RuleGroup::Custom => "User-defined regex and dictionary rules, always enabled",
        }
    }
}

impl Display for RuleGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.to_possible_value() {
            Some(value) => f.write_str(value.get_name()),
            None => write!(f, "{self:?}"),
        }
    }
}

/// One built-in rule as declared in the table. `capture_group` 0 redacts the whole match;
/// a higher number redacts only that group (used where the pattern needs context such as
/// the word "passport" that must survive). `validator` carries the name of the check for the
/// rules reference. `description`, `keyword` and `example` feed `docs/local-rules.md`.
pub struct RuleSpec {
    pub name: &'static str,
    pub group: RuleGroup,
    pub pattern: &'static str,
    pub capture_group: usize,
    pub validator: Option<(&'static str, Validator)>,
    // The three fields below are read only by the tests and the `docs/local-rules.md`
    // generator; this is a binary crate, so without the attribute `-D warnings` fails on
    // "field is never read" (same idiom as `file_converters/pdf.rs`).
    /// True when the pattern only matches next to a context word (`passport`, `bsn`, ...),
    /// which also means a CSV cell holding the bare value is not found.
    #[cfg_attr(not(test), allow(dead_code))]
    pub keyword: bool,
    /// One line, no trailing period: what the rule matches.
    #[cfg_attr(not(test), allow(dead_code))]
    pub description: &'static str,
    #[cfg_attr(not(test), allow(dead_code))]
    pub example: Example,
}

/// A value the rule redacts, for the reference and for the self-test of the table.
pub enum Example {
    /// From a public specification or vendor documentation: the worked value, then the URL
    /// that publishes it. The rules reference prints the URL beside the value so nobody
    /// mistakes it for a live identifier without also seeing where it came from.
    #[cfg_attr(not(test), allow(dead_code))]
    Published(&'static str, &'static str),
    /// Made up for the tests (checksums computed from the published formula).
    Synthetic(&'static str),
}

impl Example {
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn text(&self) -> &'static str {
        match self {
            Example::Published(text, _) | Example::Synthetic(text) => text,
        }
    }
}

#[derive(Debug)]
struct CompiledRule {
    name: RuleName,
    regex: Regex,
    capture_group: usize,
    validator: Option<Validator>,
    /// When true, a match is only kept if the characters immediately surrounding it (if
    /// any) are not word characters. Used for dictionary rules, whose alternatives may
    /// start or end with punctuation (`#tag`, `C++`) that `\b` cannot anchor on.
    whole_word: bool,
}

#[derive(Debug)]
pub struct RuleSet {
    rules: Vec<CompiledRule>,
}

/// Upper bound on the compiled size of one regex, built-in or user-supplied.
pub const REGEX_SIZE_LIMIT: usize = 1 << 20;

fn compile(name: &str, pattern: &str, case_insensitive: bool) -> Result<Regex, LocalRulesError> {
    RegexBuilder::new(pattern)
        .size_limit(REGEX_SIZE_LIMIT)
        .case_insensitive(case_insensitive)
        .build()
        .map_err(|err| LocalRulesError::InvalidRegex {
            rule: name.to_string(),
            reason: err.to_string(),
        })
}

/// True if the characters immediately outside `start..end` (if any) are not word
/// characters, matching `\b` semantics without requiring the matched text itself to
/// start or end on a word character. Lets a dictionary entry like `#tag` or `C++` match
/// even though its first or last character is not alphanumeric.
fn is_word_boundary(text: &str, start: usize, end: usize) -> bool {
    let before_is_word = text[..start]
        .chars()
        .next_back()
        .is_some_and(|c| c.is_alphanumeric() || c == '_');
    let after_is_word = text[end..]
        .chars()
        .next()
        .is_some_and(|c| c.is_alphanumeric() || c == '_');
    !before_is_word && !after_is_word
}

impl RuleSet {
    /// Compiles the built-in rules of the enabled groups, in declaration order.
    pub fn new(groups: &BTreeSet<RuleGroup>) -> Result<Self, LocalRulesError> {
        let mut rules = Vec::new();
        for spec in builtin_rules()
            .iter()
            .filter(|spec| groups.contains(&spec.group))
        {
            rules.push(CompiledRule {
                name: RuleName::new(spec.name),
                regex: compile(spec.name, spec.pattern, false)?,
                capture_group: spec.capture_group,
                validator: spec.validator.map(|(_, validate)| validate),
                whole_word: false,
            });
        }
        Ok(Self { rules })
    }

    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    /// Built-in rules of the enabled groups followed by the user's rules. Names must be
    /// unique across both sets so reports are unambiguous.
    pub fn with_user_rules(
        groups: &BTreeSet<RuleGroup>,
        user_rules: &[UserRule],
    ) -> Result<Self, LocalRulesError> {
        let mut set = Self::new(groups)?;
        let mut names: BTreeSet<RuleName> = set.rules.iter().map(|r| r.name.clone()).collect();
        for rule in user_rules {
            if !names.insert(rule.name.clone()) {
                return Err(LocalRulesError::DuplicateRule {
                    name: rule.name.to_string(),
                });
            }
            let (pattern, whole_word) = match &rule.matcher {
                UserMatcher::Regex(pattern) => (pattern.clone(), false),
                UserMatcher::Dictionary(words) => {
                    if words.is_empty() {
                        return Err(LocalRulesError::EmptyDictionary {
                            name: rule.name.to_string(),
                        });
                    }
                    // Longest first, so the alternation prefers e.g. `#tagline` over `#tag`
                    // when both are present and the input is `#tagline`.
                    let mut words = words.clone();
                    words.sort_unstable_by_key(|w| std::cmp::Reverse(w.len()));
                    let escaped: Vec<String> = words.iter().map(|w| regex::escape(w)).collect();
                    (format!("(?:{})", escaped.join("|")), true)
                }
            };
            set.rules.push(CompiledRule {
                name: rule.name.clone(),
                regex: compile(rule.name.as_str(), &pattern, rule.case_insensitive)?,
                capture_group: 0,
                validator: None,
                whole_word,
            });
        }
        Ok(set)
    }

    /// Every validated match of every rule, merged so no two findings overlap.
    pub fn find(&self, text: &str) -> Vec<Finding> {
        let mut findings = Vec::new();
        for rule in &self.rules {
            for captures in rule.regex.captures_iter(text) {
                let Some(matched) = captures.get(rule.capture_group) else {
                    continue;
                };
                // A pattern that can match the empty string (a user rule such as `a*`) yields
                // one zero-length match per position; redacting those inserts the token
                // between every character of the input.
                if matched.is_empty() {
                    continue;
                }
                if rule
                    .validator
                    .is_some_and(|validate| !validate(matched.as_str()))
                {
                    continue;
                }
                if rule.whole_word && !is_word_boundary(text, matched.start(), matched.end()) {
                    continue;
                }
                findings.push(Finding {
                    start: matched.start(),
                    end: matched.end(),
                    rule: rule.name.clone(),
                });
            }
        }
        merge_findings(findings)
    }

    pub fn redact(&self, text: &str) -> (String, Vec<Finding>) {
        let findings = self.find(text);
        (apply_redaction(text, &findings, REDACTED), findings)
    }
}

#[cfg(test)]
pub(super) mod test_support {
    use super::{compile, RuleGroup, RuleSet, RuleSpec};
    use std::sync::OnceLock;

    /// Every built-in group, compiled once per test binary: the table is large and the
    /// per-rule tests below call this hundreds of times.
    pub(super) fn all_rules() -> &'static RuleSet {
        static ALL: OnceLock<RuleSet> = OnceLock::new();
        ALL.get_or_init(|| RuleSet::new(&RuleGroup::all()).unwrap())
    }

    pub(super) fn redact_all(text: &str) -> String {
        all_rules().redact(text).0
    }

    pub(super) fn assert_redacts(text: &str, expected: &str) {
        assert_eq!(redact_all(text), expected, "input: {text}");
    }

    pub(super) fn assert_untouched(text: &str) {
        assert_eq!(redact_all(text), text, "input: {text}");
    }

    /// Runs one rule on its own: its regex, its capture group and its validator, without
    /// the other rules and without the merge.
    pub(super) fn rule_matches(spec: &RuleSpec, text: &str) -> bool {
        let regex = compile(spec.name, spec.pattern, false).unwrap();
        let matched_any = regex.captures_iter(text).any(|captures| {
            captures.get(spec.capture_group).is_some_and(|matched| {
                !matched.is_empty()
                    && spec
                        .validator
                        .is_none_or(|(_, validate)| validate(matched.as_str()))
            })
        });
        matched_any
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::*;
    use super::*;

    #[test]
    fn every_rule_example_is_matched_by_its_own_rule() {
        for spec in builtin_rules() {
            assert!(
                rule_matches(spec, spec.example.text()),
                "{}: {:?}",
                spec.name,
                spec.example.text()
            );
            assert!(
                !spec.description.is_empty() && !spec.description.ends_with('.'),
                "{}: description is one line without a trailing period",
                spec.name
            );
            if spec.keyword {
                // A keyword rule only matches next to its context word (`passport`, `bsn`,
                // ...); the bare captured value on its own must not match, which is also
                // why a CSV cell holding just the value is missed.
                let regex = compile(spec.name, spec.pattern, false).unwrap();
                let value = regex
                    .captures(spec.example.text())
                    .and_then(|captures| captures.get(spec.capture_group))
                    .unwrap()
                    .as_str();
                assert!(
                    !rule_matches(spec, value),
                    "{}: matched {value:?} without its keyword",
                    spec.name
                );
            }
        }
    }

    #[test]
    fn example_text_unwraps_either_variant() {
        assert_eq!(
            Example::Published("cited", "https://example.com").text(),
            "cited"
        );
        assert_eq!(Example::Synthetic("made up").text(), "made up");
    }

    #[test]
    fn every_builtin_pattern_compiles() {
        for spec in builtin_rules() {
            RegexBuilder::new(spec.pattern)
                .size_limit(REGEX_SIZE_LIMIT)
                .build()
                .unwrap_or_else(|err| panic!("rule {} does not compile: {err}", spec.name));
        }
    }

    #[test]
    fn builtin_rule_names_are_unique() {
        let mut names: Vec<_> = builtin_rules().iter().map(|r| r.name).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), builtin_rules().len());
    }

    #[test]
    fn group_names_are_kebab_case() {
        assert_eq!(RuleGroup::PaymentCard.to_string(), "payment-card");
        assert_eq!(RuleGroup::UsIdentifiers.to_string(), "us-identifiers");
        assert_eq!(RuleGroup::WorldIdentifiers.to_string(), "world-identifiers");
        assert_eq!(RuleGroup::Postcodes.to_string(), "postcodes");
        assert_eq!(RuleGroup::BirthDates.to_string(), "birth-dates");
    }

    #[test]
    fn email() {
        assert_redacts(
            "Contact john.smith@example.com now",
            "Contact [REDACTED] now",
        );
        assert_redacts(
            "\"email\": \"a.b+tag@sub.example.org\"",
            "\"email\": \"[REDACTED]\"",
        );
        assert_untouched("not an email: user@localhost");
    }

    #[test]
    fn phone() {
        assert_redacts("call +1 (555) 123-4567 today", "call [REDACTED] today");
        assert_redacts("Tel: +44 20 7946 0958", "Tel: [REDACTED]");
        assert_redacts("phone 0049 30 901820", "phone [REDACTED]");
        assert_redacts("cell (555) 123-4567", "cell [REDACTED]");
        assert_untouched("order number 12345");
        assert_untouched("+1 111 111 1111");
    }

    #[test]
    fn payment_card() {
        assert_redacts(
            "card 4111 1111 1111 1111 exp 12/29",
            "card [REDACTED] exp 12/29",
        );
        assert_redacts("5500-0000-0000-0004", "[REDACTED]");
        assert_untouched("4111 1111 1111 1112");
    }

    #[test]
    fn phone_national_covers_common_european_forms() {
        assert_redacts("call 020 7946 0958 now", "call [REDACTED] now");
        assert_redacts("tel 01 23 45 67 89", "tel [REDACTED]");
        assert_redacts("ruf 030 901820 an", "ruf [REDACTED] an");
        assert_untouched("total 12.345.678");
        assert_untouched("am 03.04.2024");
        assert_untouched("sum 1.234.567,89");
        assert_untouched("on 2024-04-30");
    }

    #[test]
    fn phone_international_needs_a_left_boundary() {
        assert_untouched("build v1.2.3+20240115");
        assert_redacts("Tel: +44 20 7946 0958", "Tel: [REDACTED]");
    }

    #[test]
    fn payment_card_ignores_dates_and_epoch_timestamps() {
        assert_untouched("Report 2024-04-30 2024-05-01");
        assert_untouched("ts 1725700000004");
    }

    #[test]
    fn payment_card_covers_grouped_and_unbroken_numbers() {
        assert_redacts("4111-1111-1111-1111", "[REDACTED]");
        assert_redacts("4111111111111111", "[REDACTED]");
        assert_redacts("amex 3782 822463 10005", "amex [REDACTED]");
        assert_untouched("4111111111111112");
    }

    #[test]
    fn probe_line_keeps_dates_and_redacts_the_phone_and_card() {
        assert_redacts(
            "Report 2024-04-30 2024-05-01 ts 1725700000004 v1.2.3+20240115 call 020 7946 0958 card 4111 1111 1111 1111",
            "Report 2024-04-30 2024-05-01 ts 1725700000004 v1.2.3+20240115 call [REDACTED] card [REDACTED]",
        );
    }

    #[test]
    fn private_key_block_covers_pgp_blocks() {
        assert_redacts(
            "-----BEGIN PGP PRIVATE KEY BLOCK-----\nlQOYBF\n-----END PGP PRIVATE KEY BLOCK-----\nafter",
            "[REDACTED]\nafter",
        );
    }

    #[test]
    fn iban() {
        assert_redacts("IBAN GB82 WEST 1234 5698 7654 32.", "IBAN [REDACTED].");
        assert_redacts("DE89370400440532013000", "[REDACTED]");
        assert_untouched("GB82 WEST 1234 5698 7654 33");
    }

    #[test]
    fn network() {
        assert_redacts(
            "from 192.168.0.1 and 2001:db8::1",
            "from [REDACTED] and [REDACTED]",
        );
        assert_redacts("mac 00:1A:2B:3C:4D:5E", "mac [REDACTED]");
        assert_untouched("version 1.2.3.400");
    }

    #[test]
    fn url_credentials() {
        assert_redacts(
            "https://alice:s3cret@example.com/path",
            "https://[REDACTED]@example.com/path",
        );
        assert_untouched("https://example.com/path");
    }

    #[test]
    fn secrets() {
        assert_redacts("key AKIAIOSFODNN7EXAMPLE", "key [REDACTED]");
        assert_redacts("AIzaSyA1234567890abcdefghijklmnopqrstuv", "[REDACTED]");
        assert_untouched("AIzaSyA-1234567890abcdefghijklmnopqrstuvw");
        assert_redacts("ghp_abcdefghijklmnopqrstuvwxyz0123456789", "[REDACTED]");
        assert_redacts("xoxb-123456789012-abcdefghij", "[REDACTED]");
        assert_redacts("sk_live_abcdefghijklmnop", "[REDACTED]");
        assert_redacts(
            "Authorization: Bearer abc.def.ghi",
            "Authorization: Bearer [REDACTED]",
        );
        assert_redacts(
            "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0In0.abc123def456",
            "[REDACTED]",
        );
        assert_redacts(
            "-----BEGIN RSA PRIVATE KEY-----\nMIIB\nAB==\n-----END RSA PRIVATE KEY-----\nafter",
            "[REDACTED]\nafter",
        );
        assert_redacts(
            "aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "aws_secret_access_key = [REDACTED]",
        );
        assert_redacts(
            "\"aws_secret_access_key\": \"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKE/\"",
            "\"aws_secret_access_key\": \"[REDACTED]\"",
        );
        assert_redacts(
            "aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKE/",
            "aws_secret_access_key = [REDACTED]",
        );
        assert_redacts(
            "\"api_key\": \"AIzaSyA1234567890abcdefghijklmnopqrstu-\"",
            "\"api_key\": \"[REDACTED]\"",
        );
    }

    #[test]
    fn us_identifiers() {
        assert_redacts("SSN 123-45-6789", "SSN [REDACTED]");
        assert_untouched("SSN 000-45-6789");
        assert_redacts("Passport number: 123456789", "Passport number: [REDACTED]");
        assert_untouched("reference 123456789");
    }

    #[test]
    fn eu_identifiers() {
        assert_redacts("VAT DE136695976", "VAT [REDACTED]");
        assert_untouched("VAT DE136695977");
        assert_redacts("DNI 12345678Z", "DNI [REDACTED]");
        assert_redacts("NIE X1234567L", "NIE [REDACTED]");
        assert_redacts("CF RSSMRA85M01H501Q", "CF [REDACTED]");
        assert_redacts("BSN 111222333", "BSN [REDACTED]");
        assert_redacts("bsn,111222333", "bsn,[REDACTED]");
        assert_untouched("order 100000009");
        assert_redacts("NINO AB 12 34 56 C", "NINO [REDACTED]");
        assert_redacts("NIR 2 69 05 49 588 157 80", "NIR [REDACTED]");
        assert_redacts("Steuer-ID 86095742719", "Steuer-ID [REDACTED]");
        assert_untouched("ref 86095742719");
    }

    #[test]
    fn overlapping_rules_replace_once() {
        let (redacted, findings) = all_rules().redact("GB82WEST12345698765432");
        assert_eq!(redacted, "[REDACTED]");
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].rule.as_str(), "iban");
    }

    #[test]
    fn findings_carry_byte_offsets_of_the_original_text() {
        let text = "héllo a@b.io";
        let findings = all_rules().find(text);
        assert_eq!(&text[findings[0].start..findings[0].end], "a@b.io");
    }

    #[test]
    fn disabled_groups_do_not_match() {
        let mut groups = RuleGroup::all();
        groups.remove(&RuleGroup::Email);
        let rules = RuleSet::new(&groups).unwrap();
        assert_eq!(rules.redact("a@b.io").0, "a@b.io");
    }

    #[test]
    fn no_groups_gives_an_empty_set() {
        assert!(RuleSet::new(&BTreeSet::new()).unwrap().is_empty());
    }

    #[test]
    fn structured_text_keeps_its_syntax() {
        assert_redacts(
            "{\"email\":\"a@b.io\",\"ip\":\"10.0.0.1\"}",
            "{\"email\":\"[REDACTED]\",\"ip\":\"[REDACTED]\"}",
        );
        assert_redacts("a@b.io,+1 555 123 4567\n", "[REDACTED],[REDACTED]\n");
        assert_redacts(
            "<a href=\"mailto:a@b.io\">",
            "<a href=\"mailto:[REDACTED]\">",
        );
    }

    #[test]
    fn user_regex_rule_is_applied() {
        let rule = UserRule {
            name: RuleName::new("employee-id"),
            matcher: UserMatcher::Regex(r"\bEMP-\d{6}\b".to_string()),
            case_insensitive: false,
        };
        let rules = RuleSet::with_user_rules(&BTreeSet::new(), &[rule]).unwrap();
        let (redacted, findings) = rules.redact("badge EMP-123456 issued");
        assert_eq!(redacted, "badge [REDACTED] issued");
        assert_eq!(findings[0].rule.as_str(), "employee-id");
    }

    #[test]
    fn user_dictionary_rule_matches_whole_words_case_insensitively() {
        let rule = UserRule {
            name: RuleName::new("codenames"),
            matcher: UserMatcher::Dictionary(vec!["Bluebird".to_string(), "Kestrel".to_string()]),
            case_insensitive: true,
        };
        let rules = RuleSet::with_user_rules(&BTreeSet::new(), &[rule]).unwrap();
        assert_eq!(
            rules.redact("project BLUEBIRD and kestrel").0,
            "project [REDACTED] and [REDACTED]"
        );
        assert_eq!(rules.redact("bluebirds fly").0, "bluebirds fly");
    }

    #[test]
    fn user_dictionary_words_are_escaped() {
        let rule = UserRule {
            name: RuleName::new("special"),
            matcher: UserMatcher::Dictionary(vec!["a.b".to_string()]),
            case_insensitive: false,
        };
        let rules = RuleSet::with_user_rules(&BTreeSet::new(), &[rule]).unwrap();
        assert_eq!(rules.redact("a.b axb").0, "[REDACTED] axb");
    }

    #[test]
    fn user_dictionary_matches_a_word_with_punctuation_at_its_edges() {
        let rule = UserRule {
            name: RuleName::new("hashtags"),
            matcher: UserMatcher::Dictionary(vec!["#tag".to_string()]),
            case_insensitive: false,
        };
        let rules = RuleSet::with_user_rules(&BTreeSet::new(), &[rule]).unwrap();
        assert_eq!(
            rules.redact("wear a #tag today").0,
            "wear a [REDACTED] today"
        );
    }

    #[test]
    fn user_dictionary_matches_a_word_ending_in_punctuation() {
        let rule = UserRule {
            name: RuleName::new("langs"),
            matcher: UserMatcher::Dictionary(vec!["C++".to_string()]),
            case_insensitive: false,
        };
        let rules = RuleSet::with_user_rules(&BTreeSet::new(), &[rule]).unwrap();
        assert_eq!(rules.redact("we use C++ here").0, "we use [REDACTED] here");
    }

    #[test]
    fn user_dictionary_redacts_repeated_adjacent_matches() {
        let rule = UserRule {
            name: RuleName::new("hashtags"),
            matcher: UserMatcher::Dictionary(vec!["#tag".to_string()]),
            case_insensitive: false,
        };
        let rules = RuleSet::with_user_rules(&BTreeSet::new(), &[rule]).unwrap();
        assert_eq!(rules.redact("#tag #tag").0, "[REDACTED] [REDACTED]");
    }

    #[test]
    fn user_dictionary_still_requires_a_whole_word_for_plain_words() {
        let rule = UserRule {
            name: RuleName::new("tag"),
            matcher: UserMatcher::Dictionary(vec!["tag".to_string()]),
            case_insensitive: false,
        };
        let rules = RuleSet::with_user_rules(&BTreeSet::new(), &[rule]).unwrap();
        assert_eq!(rules.redact("hashtags").0, "hashtags");
        assert_eq!(rules.redact("tag_line").0, "tag_line");
    }

    #[test]
    fn user_dictionary_prefers_the_longest_alternative() {
        let rule = UserRule {
            name: RuleName::new("hashtags"),
            matcher: UserMatcher::Dictionary(vec!["#tag".to_string(), "#tagline".to_string()]),
            case_insensitive: false,
        };
        let rules = RuleSet::with_user_rules(&BTreeSet::new(), &[rule]).unwrap();
        assert_eq!(rules.redact("#tagline").0, "[REDACTED]");
    }

    #[test]
    fn user_rule_with_invalid_regex_is_rejected() {
        let rule = UserRule {
            name: RuleName::new("broken"),
            matcher: UserMatcher::Regex("(".to_string()),
            case_insensitive: false,
        };
        let err = RuleSet::with_user_rules(&BTreeSet::new(), &[rule]).unwrap_err();
        assert!(
            matches!(err, LocalRulesError::InvalidRegex { ref rule, .. } if rule == "broken"),
            "{err}"
        );
    }

    #[test]
    fn user_regex_matching_the_empty_string_leaves_the_text_untouched() {
        let rule = UserRule {
            name: RuleName::new("empty"),
            matcher: UserMatcher::Regex("a*".to_string()),
            case_insensitive: false,
        };
        let rules = RuleSet::with_user_rules(&BTreeSet::new(), &[rule]).unwrap();
        let (redacted, findings) = rules.redact("xyz");
        assert_eq!(redacted, "xyz");
        assert!(findings.is_empty(), "{findings:?}");
        assert_eq!(rules.redact("aaa xyz").0, "[REDACTED] xyz");
    }

    #[test]
    fn user_dictionary_does_not_match_next_to_a_non_ascii_letter() {
        let word_rule = |word: &str| UserRule {
            name: RuleName::new("tags"),
            matcher: UserMatcher::Dictionary(vec![word.to_string()]),
            case_insensitive: false,
        };
        let rules = RuleSet::with_user_rules(&BTreeSet::new(), &[word_rule("tag")]).unwrap();
        assert_eq!(rules.redact("étag").0, "étag");
        assert_eq!(rules.redact("tagé").0, "tagé");
        let rules = RuleSet::with_user_rules(&BTreeSet::new(), &[word_rule("étag")]).unwrap();
        assert_eq!(rules.redact("étag").0, "[REDACTED]");
    }

    #[test]
    fn two_user_rules_with_the_same_name_are_rejected() {
        let rule = |pattern: &str| UserRule {
            name: RuleName::new("employee-id"),
            matcher: UserMatcher::Regex(pattern.to_string()),
            case_insensitive: false,
        };
        let err = RuleSet::with_user_rules(&BTreeSet::new(), &[rule("a"), rule("b")]).unwrap_err();
        assert!(
            matches!(err, LocalRulesError::DuplicateRule { ref name } if name == "employee-id"),
            "{err}"
        );
    }

    #[test]
    fn user_rule_named_like_a_builtin_is_rejected() {
        let rule = UserRule {
            name: RuleName::new("email"),
            matcher: UserMatcher::Regex("x".to_string()),
            case_insensitive: false,
        };
        let err = RuleSet::with_user_rules(&RuleGroup::all(), &[rule]).unwrap_err();
        assert!(
            matches!(err, LocalRulesError::DuplicateRule { .. }),
            "{err}"
        );
    }
}

/// Renders `docs/local-rules.md`, the user-facing list of every built-in rule, from the
/// table. Test-only: the file is regenerated on purpose and checked in.
#[cfg(test)]
mod reference {
    use super::table::builtin_rules;
    use super::{Example, RuleGroup, RuleSpec};
    use clap::ValueEnum;
    use std::fmt::Write;

    const PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/docs/local-rules.md");

    /// Markdown table cell: pipes and newlines would break the row.
    fn cell(text: &str) -> String {
        text.replace('|', "\\|").replace('\n', "\\n")
    }

    fn example(example: &Example) -> String {
        match example {
            Example::Published(text, citation) => {
                format!("`{}` (published, [source]({citation}))", cell(text))
            }
            Example::Synthetic(text) => format!("`{}` (synthetic)", cell(text)),
        }
    }

    fn render_rules_reference() -> String {
        let rules = builtin_rules();
        let mut out = String::from("# Local rules reference\n\n");
        out.push_str(
            "Every built-in rule of the `local-rules` redacter, grouped as the `--local-rules` \
             and `--local-rules-disable` options see them. A rule with a validator only redacts \
             values that pass the named check; a rule with a keyword only redacts values within \
             a few characters of a context word such as `passport` or `NIF`. A synthetic example \
             is made up for the tests, with check digits computed from the published formula; a \
             published example is a real worked value from the cited public specification or \
             vendor documentation. Generated from the rule table by \
             `cargo test rules_reference_regenerate -- --ignored`; do not edit by hand.\n\n",
        );
        out.push_str("| Group | Rules | Purpose |\n|---|---|---|\n");
        for group in RuleGroup::value_variants() {
            let count = if *group == RuleGroup::Custom {
                "user-defined".to_string()
            } else {
                rules
                    .iter()
                    .filter(|rule| rule.group == *group)
                    .count()
                    .to_string()
            };
            writeln!(out, "| `{group}` | {count} | {} |", group.purpose()).unwrap();
        }
        for group in RuleGroup::value_variants() {
            let members: Vec<&RuleSpec> =
                rules.iter().filter(|rule| rule.group == *group).collect();
            if members.is_empty() {
                continue;
            }
            write!(
                out,
                "\n## `{group}`\n\n{}.\n\n| Rule | Description | Validator | Keyword | Example |\n|---|---|---|---|---|\n",
                group.purpose()
            )
            .unwrap();
            for rule in members {
                let validator = rule
                    .validator
                    .map_or_else(|| "no".to_string(), |(name, _)| format!("yes ({name})"));
                let keyword = if rule.keyword { "yes" } else { "no" };
                writeln!(
                    out,
                    "| `{}` | {} | {validator} | {keyword} | {} |",
                    rule.name,
                    cell(rule.description),
                    example(&rule.example)
                )
                .unwrap();
            }
        }
        out
    }

    #[test]
    fn every_group_has_a_purpose() {
        for group in RuleGroup::value_variants() {
            let purpose = group.purpose();
            assert!(!purpose.is_empty() && !purpose.ends_with('.'), "{group}");
        }
    }

    #[test]
    fn rules_reference_is_up_to_date() {
        // Git may check the file out with CRLF line endings on Windows; compare the text only.
        let committed = std::fs::read_to_string(PATH)
            .unwrap_or_default()
            .replace("\r\n", "\n");
        assert!(
            committed == render_rules_reference(),
            "docs/local-rules.md is stale: run `cargo test rules_reference_regenerate -- --ignored` to regenerate"
        );
    }

    #[test]
    #[ignore = "writes docs/local-rules.md; run on purpose to regenerate it"]
    fn rules_reference_regenerate() {
        std::fs::create_dir_all(concat!(env!("CARGO_MANIFEST_DIR"), "/docs")).unwrap();
        std::fs::write(PATH, render_rules_reference()).unwrap();
    }
}
