use super::error::LocalRulesError;
use super::user_rules::{UserMatcher, UserRule};
use super::validators::Validator;
use crate::redacters::text_spans::{merge_findings, Finding, RuleName};
use crate::redacters::Category;
use clap::ValueEnum;
use regex::{Match, Regex, RegexBuilder};
use std::borrow::Cow;
use std::collections::BTreeSet;
use std::fmt::Display;
use std::sync::LazyLock;

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

    /// The category of what the group finds.
    pub fn category(self) -> Category {
        match self {
            RuleGroup::Email | RuleGroup::Phone => Category::Contact,
            RuleGroup::PaymentCard | RuleGroup::Iban => Category::Financial,
            RuleGroup::Network => Category::Network,
            RuleGroup::Url | RuleGroup::Secrets => Category::Secrets,
            RuleGroup::UsIdentifiers | RuleGroup::EuIdentifiers | RuleGroup::WorldIdentifiers => {
                Category::IdNumber
            }
            RuleGroup::Postcodes => Category::Address,
            RuleGroup::BirthDates => Category::DateOfBirth,
            RuleGroup::Custom => Category::Other,
        }
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
            RuleGroup::Secrets => {
                "Passwords and keys in `key=value` lines, vendor API keys and tokens, authorization headers and private keys"
            }
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

impl RuleSpec {
    /// Whether the rule finds a password, a key or a token.
    #[cfg(feature = "local-credential-ml")]
    fn finds_credential(&self) -> bool {
        self.group == RuleGroup::Secrets || self.name == URL_CREDENTIALS_RULE
    }
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

/// Where the search for the next value of a rule starts after a value.
#[derive(Debug, Clone, Copy)]
enum Resume {
    /// At the end of the captured value: a built-in pattern may consume context around its
    /// value (the separator before and after a postcode) that the next value needs as its own.
    AfterValue,
    /// At the end of the whole match: a user pattern such as `(\d{3})\d*-ID` must not find a
    /// second value in the rest of the text it already matched.
    AfterMatch,
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
    /// True for a built-in rule: its pattern is searched in the text returned by
    /// [`mask_marks_outside_words`], so that a mark after a symbol does not hide the `\b`
    /// before the value. A user rule is searched in the text as written, because its
    /// pattern or dictionary may name such a mark itself.
    marks_end_words: bool,
    category: Category,
    resume: Resume,
}

/// The rule as the user wrote it. Fails when the regex does not compile, when it has no
/// capture group `capture_group`, when `validator` names no built-in value check, and when
/// an example is not found or a counter-example is. A user mistake stops the run here
/// rather than leaving a rule that silently finds nothing, or too much.
impl TryFrom<&UserRule> for CompiledRule {
    type Error = LocalRulesError;

    fn try_from(rule: &UserRule) -> Result<Self, Self::Error> {
        let (pattern, default_whole_word) = match &rule.matcher {
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
        let regex = compile(rule.name.as_str(), &pattern, rule.case_insensitive)?;
        if rule.capture_group >= regex.captures_len() {
            return Err(LocalRulesError::CaptureGroupOutOfRange {
                name: rule.name.to_string(),
                capture_group: rule.capture_group,
                groups: regex.captures_len(),
            });
        }
        let validator = rule
            .validator
            .as_ref()
            .map(|named| {
                RuleSet::builtin_validator(named).ok_or_else(|| LocalRulesError::UnknownValidator {
                    rule: rule.name.to_string(),
                    validator: named.to_string(),
                })
            })
            .transpose()?;
        let compiled = Self {
            name: rule.name.clone(),
            regex,
            capture_group: rule.capture_group,
            validator,
            whole_word: rule.whole_word.unwrap_or(default_whole_word),
            marks_end_words: false,
            category: rule.category,
            resume: Resume::AfterMatch,
        };
        for example in &rule.examples {
            if compiled.findings(example, example).next().is_none() {
                return Err(LocalRulesError::ExampleNotMatched {
                    rule: rule.name.to_string(),
                    example: example.clone(),
                });
            }
        }
        for example in &rule.counter_examples {
            if compiled.findings(example, example).next().is_some() {
                return Err(LocalRulesError::CounterExampleMatched {
                    rule: rule.name.to_string(),
                    example: example.clone(),
                });
            }
        }
        Ok(compiled)
    }
}

impl CompiledRule {
    /// The findings of this rule alone in `text`: its values that pass its validator and,
    /// for a whole-word rule, stand on word boundaries. Not merged with other findings.
    /// `searched` is the text the pattern runs on and must have the byte offsets of `text`:
    /// `text` itself, or for a built-in rule its [`mask_marks_outside_words`] form.
    fn findings<'t>(
        &'t self,
        text: &'t str,
        searched: &'t str,
    ) -> impl Iterator<Item = Finding> + 't {
        self.values(searched)
            .filter(|matched| {
                self.validator
                    .is_none_or(|validate| validate(&text[matched.range()]))
                    && (!self.whole_word || is_word_boundary(text, matched.start(), matched.end()))
            })
            .map(|matched| Finding {
                start: matched.start(),
                end: matched.end(),
                rule: self.name.clone(),
                score: None,
            })
    }

    /// The non-empty text each match of the rule captured in its capture group.
    ///
    /// The search resumes where [`Resume`] says. A match with an empty capture, such as one of
    /// a user pattern like `a*`, yields nothing and the search moves on by one character.
    fn values<'t>(&'t self, text: &'t str) -> impl Iterator<Item = Match<'t>> + 't {
        let mut from = 0;
        std::iter::from_fn(move || {
            while from <= text.len() {
                let captures = self.regex.captures_at(text, from)?;
                let whole = captures.get(0)?;
                let value = captures
                    .get(self.capture_group)
                    .filter(|value| !value.is_empty());
                from = match value {
                    Some(value) => match self.resume {
                        Resume::AfterValue => value.end(),
                        Resume::AfterMatch => whole.end(),
                    },
                    None if whole.is_empty() => text[whole.end()..]
                        .chars()
                        .next()
                        .map_or(text.len() + 1, |next| whole.end() + next.len_utf8()),
                    None => whole.end(),
                };
                if value.is_some() {
                    return value;
                }
            }
            None
        })
    }
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
/// characters, with the same notion of a word character as the regex `\b`, without
/// requiring the matched text itself to start or end on a word character. Lets a
/// dictionary entry like `#tag` or `C++` match even though its first or last character is
/// not alphanumeric.
///
/// Before the match, combining marks are skipped to reach the character they belong to, so
/// a mark after a symbol (`\u{26a0}\u{fe0f}`) or at the start of the text does not make the
/// match look like the continuation of a word. After the match a mark counts as a word
/// character, because it belongs to the last character of the match.
fn is_word_boundary(text: &str, start: usize, end: usize) -> bool {
    let before_is_word = text[..start]
        .chars()
        .rev()
        .find(|c| !is_combining_mark(*c))
        .is_some_and(is_word_char);
    let after_is_word = text[end..].chars().next().is_some_and(is_word_char);
    !before_is_word && !after_is_word
}

/// Exactly the characters the regex `\w` matches, so that whole-word checks agree with the
/// built-in rules' `\b`. The standard library has no equivalent: its `is_alphanumeric`
/// misses combining marks and joiners, and includes superscript digits.
fn is_word_char(c: char) -> bool {
    static WORD_CHAR: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"^\w$").expect("a fixed, valid pattern"));
    WORD_CHAR.is_match(c.encode_utf8(&mut [0; 4]))
}

/// `text` with every combining mark that does not belong to a word replaced by a symbol of the
/// same UTF-8 width, so byte offsets stay valid and the symbol is neither a word character nor
/// whitespace. Such a mark is the emoji variation selector after `\u{26a0}`, or any mark at the
/// start of the text, and the regex `\b` counts it as a word character: without this, `\b`
/// finds no boundary between `\u{26a0}\u{fe0f}` and the value that follows.
///
/// A mark after a word character stays, as part of that word, except from an enclosing mark on:
/// the keycap in `1\u{fe0f}\u{20e3}` turns the digit into a symbol.
fn mask_marks_outside_words(text: &str) -> Cow<'_, str> {
    static MARK_RUN: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"\p{M}+").expect("a fixed, valid pattern"));
    static ENCLOSING_MARK: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"\p{Me}").expect("a fixed, valid pattern"));

    let mut runs = MARK_RUN.find_iter(text).peekable();
    if runs.peek().is_none() {
        return Cow::Borrowed(text);
    }
    let mut masked = String::with_capacity(text.len());
    let mut copied = 0;
    for run in runs {
        let follows_word = text[..run.start()]
            .chars()
            .next_back()
            .is_some_and(is_word_char);
        let masked_from = if follows_word {
            ENCLOSING_MARK
                .find(run.as_str())
                .map(|enclosing| run.start() + enclosing.start())
        } else {
            Some(run.start())
        };
        if let Some(from) = masked_from {
            masked.push_str(&text[copied..from]);
            masked.extend(
                text[from..run.end()]
                    .chars()
                    .map(|mark| match mark.len_utf8() {
                        2 => '\u{b7}',
                        3 => '\u{2022}',
                        _ => '\u{1f600}',
                    }),
            );
            copied = run.end();
        }
    }
    masked.push_str(&text[copied..]);
    Cow::Owned(masked)
}

fn is_combining_mark(c: char) -> bool {
    static COMBINING_MARK: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"^\p{M}$").expect("a fixed, valid pattern"));
    COMBINING_MARK.is_match(c.encode_utf8(&mut [0; 4]))
}

/// How closely a built-in rule insists on a value that looks like a credential.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Strictness {
    /// The rule as declared, validators included.
    Strict,
    /// For a line something else has judged to hold a credential: no validators and no
    /// minimum length of a quoted value.
    #[cfg(feature = "local-credential-ml")]
    Lenient,
}

/// `pattern` with the minimum length of a quoted value removed: 8 characters for the quoted
/// `password = "..."` rules, 7 for the braced `Pwd={...}` one. The first character of a value
/// stays required. Every built-in pattern that ends a value with `{7,}` or `{6,}` is one of
/// those five rules, and the rewrite touches nothing else.
#[cfg(feature = "local-credential-ml")]
fn without_minimum_quoted_length(pattern: &str) -> String {
    pattern.replace("{7,}", "*").replace("{6,}", "*")
}

/// The built-in rule that matches a whole PEM or PGP private key block, which spans lines.
pub const PRIVATE_KEY_BLOCK_RULE: &str = "private-key-block";

/// The built-in rule that matches the `user:password` of a URL.
pub const URL_CREDENTIALS_RULE: &str = "url-credentials";

/// The built-in rule ported from gitleaks that matches a high-entropy value after a key name. It
/// is not run on a line judged to hold a credential: without its value checks it matches the key
/// name itself (`key="ClearTextPassword"` in `<add key="ClearTextPassword" value="...">`).
pub const GENERIC_API_KEY_RULE: &str = "generic-api-key";

impl RuleSet {
    /// The category of a finding named `rule`: that of the group of the built-in rule of the
    /// name, `other` for any other name. It knows the built-in rules only; the categories of
    /// the user's rules are in the rule set, see [`Self::category_of_rule`] and
    /// `Redacters::category_of`.
    pub fn category_of(rule: &str) -> Category {
        builtin_rules()
            .iter()
            .find(|spec| spec.name == rule)
            .map_or(Category::Other, |spec| spec.group.category())
    }

    /// The category of the findings of the rule named `rule` in this set, `other` when the set
    /// has no such rule. Unlike [`Self::category_of`] it knows the user's rules.
    pub fn category_of_rule(&self, rule: &str) -> Category {
        self.rules
            .iter()
            .find(|candidate| candidate.name.as_str() == rule)
            .map_or(Category::Other, |candidate| candidate.category)
    }

    /// The value check of the built-in rule `name`, for a user rule that wants the same check.
    /// `None` when no built-in rule has that name or the rule has no value check.
    fn builtin_validator(name: &RuleName) -> Option<Validator> {
        builtin_rules()
            .iter()
            .find(|spec| spec.name == name.as_str())
            .and_then(|spec| spec.validator)
            .map(|(_, validate)| validate)
    }

    /// Whether `name` is the name of a built-in rule, enabled or not.
    fn is_builtin_name(name: &str) -> bool {
        builtin_rules().iter().any(|spec| spec.name == name)
    }

    /// Compiles the built-in rules of the enabled groups, in declaration order.
    pub fn new(groups: &BTreeSet<RuleGroup>) -> Result<Self, LocalRulesError> {
        Self::from_builtin(|spec| groups.contains(&spec.group), Strictness::Strict)
    }

    /// The rules that find the value of a credential on one line, for a caller that already
    /// has evidence that the line holds one: every `Secrets` rule and `url-credentials`, apart
    /// from the multi-line private key block and `generic-api-key`. Value validators and the minimum length of a
    /// quoted value are off, so a short or plain value written as `password: abc` or
    /// `password = "abc"` is found on such a line; on an unjudged line they are what keep
    /// these rules from redacting prose and code.
    #[cfg(feature = "local-credential-ml")]
    pub fn credential_line_rules() -> Result<Self, LocalRulesError> {
        Self::from_builtin(
            |spec| {
                spec.finds_credential()
                    && spec.name != PRIVATE_KEY_BLOCK_RULE
                    && spec.name != GENERIC_API_KEY_RULE
            },
            Strictness::Lenient,
        )
    }

    /// Only the private key block rule.
    #[cfg(feature = "local-credential-ml")]
    pub fn private_key_block_rule() -> Result<Self, LocalRulesError> {
        Self::from_builtin(
            |spec| spec.name == PRIVATE_KEY_BLOCK_RULE,
            Strictness::Strict,
        )
    }

    fn from_builtin(
        keep: impl Fn(&RuleSpec) -> bool,
        strictness: Strictness,
    ) -> Result<Self, LocalRulesError> {
        let mut rules = Vec::new();
        for spec in builtin_rules().iter().filter(|spec| keep(spec)) {
            let pattern = match strictness {
                Strictness::Strict => spec.pattern.to_string(),
                #[cfg(feature = "local-credential-ml")]
                Strictness::Lenient => without_minimum_quoted_length(spec.pattern),
            };
            rules.push(CompiledRule {
                name: RuleName::new(spec.name),
                regex: compile(spec.name, &pattern, false)?,
                capture_group: spec.capture_group,
                validator: spec
                    .validator
                    .filter(|_| strictness == Strictness::Strict)
                    .map(|(_, validate)| validate),
                whole_word: false,
                marks_end_words: true,
                category: spec.group.category(),
                resume: Resume::AfterValue,
            });
        }
        Ok(Self { rules })
    }

    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    /// Built-in rules of the enabled groups followed by the user's rules. A user rule may not
    /// take the name of any built-in rule, enabled or not, nor of another user rule: a finding
    /// is placed in a category by its rule name, and a shared name would place a user's rule
    /// in the category of the built-in one.
    pub fn with_user_rules(
        groups: &BTreeSet<RuleGroup>,
        user_rules: &[UserRule],
    ) -> Result<Self, LocalRulesError> {
        let mut set = Self::new(groups)?;
        let mut names: BTreeSet<RuleName> = BTreeSet::new();
        for rule in user_rules {
            if Self::is_builtin_name(rule.name.as_str()) {
                return Err(LocalRulesError::BuiltinRuleName {
                    name: rule.name.to_string(),
                });
            }
            if !names.insert(rule.name.clone()) {
                return Err(LocalRulesError::DuplicateRule {
                    name: rule.name.to_string(),
                });
            }
            set.rules.push(CompiledRule::try_from(rule)?);
        }
        Ok(set)
    }

    /// Every validated match of every rule, merged so no two findings overlap.
    pub fn find(&self, text: &str) -> Vec<Finding> {
        let masked = mask_marks_outside_words(text);
        let findings = self
            .rules
            .iter()
            .flat_map(|rule| {
                let searched = if rule.marks_end_words { &masked } else { text };
                rule.findings(text, searched)
            })
            .collect();
        merge_findings(findings)
    }
}

#[cfg(test)]
pub(super) mod test_support {
    use super::{compile, mask_marks_outside_words, RuleGroup, RuleSet, RuleSpec};
    use crate::redacters::text_spans::{apply_redaction, Finding, REDACTED};
    use std::sync::OnceLock;

    /// The text with every finding of `rules` replaced by `[REDACTED]`, plus the findings.
    pub(super) fn redact(rules: &RuleSet, text: &str) -> (String, Vec<Finding>) {
        let findings = rules.find(text);
        (apply_redaction(text, &findings, REDACTED), findings)
    }

    /// Every built-in group, compiled once per test binary: the table is large and the
    /// per-rule tests below call this hundreds of times.
    pub(super) fn all_rules() -> &'static RuleSet {
        static ALL: OnceLock<RuleSet> = OnceLock::new();
        ALL.get_or_init(|| RuleSet::new(&RuleGroup::all()).unwrap())
    }

    pub(super) fn redact_all(text: &str) -> String {
        redact(all_rules(), text).0
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
        let searched = mask_marks_outside_words(text);
        let matched_any = regex.captures_iter(&searched).any(|captures| {
            captures.get(spec.capture_group).is_some_and(|matched| {
                !matched.is_empty()
                    && spec
                        .validator
                        .is_none_or(|(_, validate)| validate(&text[matched.range()]))
            })
        });
        matched_any
    }
}

#[cfg(test)]
mod tests {
    use super::super::user_rules::load_rules_file;
    use super::test_support::*;
    use super::*;

    #[test]
    fn every_group_but_custom_finds_a_named_category() {
        for group in RuleGroup::value_variants() {
            let named = group.category() != Category::Other;
            assert_eq!(named, *group != RuleGroup::Custom, "{group}");
        }
    }

    #[test]
    fn a_built_in_rule_has_the_category_of_its_group_and_a_user_rule_is_other() {
        for spec in builtin_rules() {
            assert_eq!(
                RuleSet::category_of(spec.name),
                spec.group.category(),
                "{}",
                spec.name
            );
        }
        assert_eq!(RuleSet::category_of("generic-api-key"), Category::Secrets);
        assert_eq!(RuleSet::category_of("employee-number"), Category::Other);
    }

    #[cfg(feature = "local-credential-ml")]
    #[test]
    fn only_the_five_quoted_value_rules_lose_their_minimum_length() {
        let lenient: Vec<&str> = builtin_rules()
            .iter()
            .filter(|spec| without_minimum_quoted_length(spec.pattern) != spec.pattern)
            .map(|spec| spec.name)
            .collect();
        assert_eq!(
            lenient,
            [
                "secret-assignment-double-quoted",
                "secret-assignment-single-quoted",
                "secret-assignment-backtick-quoted",
                "secret-assignment-escaped-double-quoted",
                "secret-assignment-braced",
            ]
        );
    }

    #[cfg(feature = "local-credential-ml")]
    #[test]
    fn a_short_quoted_value_is_found_only_where_the_line_is_known_to_hold_a_credential() {
        let line = r#"password = "hunter2""#;
        assert_eq!(
            RuleSet::new(&RuleGroup::all()).unwrap().find(line),
            Vec::new()
        );
        assert_eq!(
            RuleSet::credential_line_rules().unwrap().find(line).len(),
            1
        );
    }

    #[cfg(feature = "local-credential-ml")]
    #[test]
    fn credential_line_rules_leave_out_the_generic_api_key_rule() {
        let line = r#"<add key="ClearTextPassword" value="x">"#;
        assert_eq!(
            RuleSet::new(&RuleGroup::all()).unwrap().find(line),
            Vec::new()
        );
        assert_eq!(
            RuleSet::credential_line_rules().unwrap().find(line),
            Vec::new()
        );
    }

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
    fn email_after_an_emoji_with_a_variation_selector_is_redacted() {
        assert_redacts(
            "\u{26a0}\u{fe0f}john@example.com",
            "\u{26a0}\u{fe0f}[REDACTED]",
        );
        assert_redacts("\u{26a0}john@example.com", "\u{26a0}[REDACTED]");
    }

    #[test]
    fn a_mark_that_belongs_to_a_word_still_glues_the_value_to_it() {
        assert_untouched("caf\u{65}\u{301}john@example.com");
        assert_redacts(
            "1\u{fe0f}\u{20e3}john@example.com",
            "1\u{fe0f}\u{20e3}[REDACTED]",
        );
        assert_redacts(
            "\u{301}john@example.com \u{1f600}\u{fe0f}+44 20 7946 0958",
            "\u{301}[REDACTED] \u{1f600}\u{fe0f}[REDACTED]",
        );
    }

    #[test]
    fn every_rule_example_is_found_beside_an_emoji_with_a_variation_selector() {
        let emoji = "\u{26a0}\u{fe0f}";
        let missed: Vec<String> = builtin_rules()
            .iter()
            .flat_map(|spec| {
                let example = spec.example.text();
                [
                    ("before", format!("{emoji}{example}")),
                    ("after", format!("{example}{emoji}")),
                ]
                .into_iter()
                .filter(|(_, text)| !rule_matches(spec, text))
                .map(|(side, _)| format!("{} ({side})", spec.name))
                .collect::<Vec<_>>()
            })
            .collect();
        assert!(missed.is_empty(), "{missed:#?}");
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
    fn url_credentials_with_an_email_like_password_keep_the_host() {
        assert_redacts(
            "https://alice:jane%40corp.com@example.com/path",
            "https://[REDACTED]@example.com/path",
        );
        assert_redacts(
            "ftp://alice:s3cret@localhost/path",
            "ftp://[REDACTED]@localhost/path",
        );
    }

    #[test]
    fn url_credentials_with_an_email_as_user_are_redacted_whole() {
        assert_redacts("https://bob@corp.com:pw@host/", "https://[REDACTED]@host/");
    }

    #[test]
    fn an_email_beside_a_url_with_credentials_is_still_found() {
        assert_redacts(
            "mail jane@example.com about https://alice:s3cret@example.com/path",
            "mail [REDACTED] about https://[REDACTED]@example.com/path",
        );
        assert_redacts(
            "https://alice:s3cret@example.com/path or mailto:jane@example.com",
            "https://[REDACTED]@example.com/path or mailto:[REDACTED]",
        );
        assert_redacts(
            "https://alice:s3cret@example.com/?cc=jane@example.com",
            "https://[REDACTED]@example.com/?cc=[REDACTED]",
        );
    }

    #[test]
    fn prose_and_identifiers_after_a_key_name_are_not_keys() {
        for line in [
            "Tink-written keysets on the Cloud KMS key, ChaCha20-Poly1305. Tink-written",
            "One loader name set for local private keys in both specs: `from_pkcs8_der`, `from_env`",
            "| `Aes256KeyWrap` | the key algorithm: KEY_ALG_RSA_1024 |",
            "operations, which must suit the key type: `RsaOaepSha256` for RSA keys,\n  `Aes256KeyWrap` (`A256KW`) for AES keys.",
            r#"kms_aead_mode="keyring", kms_aead_operation="encrypt", kms_aead_purpose="randomized""#,
        ] {
            assert_untouched(line);
        }
    }

    #[test]
    fn random_values_after_a_key_name_are_found_in_each_separator_form() {
        for (line, value) in [
            (r#"signing_key = "HP5YS7VR0GCE""#, "HP5YS7VR0GCE"),
            ("license_key: vsgm-6vh4-hwnx-zkxt", "vsgm-6vh4-hwnx-zkxt"),
            ("credentials=live_0crm1svz", "live_0crm1svz"),
            (
                r#"os.getenv("SIGNING_KEY", "Zq8Lw3Tn5VxB7mRc")"#,
                "Zq8Lw3Tn5VxB7mRc",
            ),
        ] {
            assert_redacts(line, &line.replacen(value, "[REDACTED]", 1));
        }
    }

    #[test]
    fn aligned_assignments_are_found() {
        assert_redacts(
            "region            = \"eu-north-1\"\nsigning_key       = \"Zq8Lw3Tn5VxB7mRc\"",
            "region            = \"eu-north-1\"\nsigning_key       = \"[REDACTED]\"",
        );
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
    fn secret_assignment_redacts_only_the_value() {
        assert_redacts("DB_PASSWORD=Sup3rS3cret!", "DB_PASSWORD=[REDACTED]");
        assert_redacts("password: hunter2Secret", "password: [REDACTED]");
        assert_redacts("password = \"tomlPass99\"", "password = \"[REDACTED]\"");
        assert_redacts(
            "spring.datasource.password=ChangeMe123",
            "spring.datasource.password=[REDACTED]",
        );
        assert_redacts(
            "x-api-key: 3c6e0b8a9c15224a8228b9a98ca1531d",
            "x-api-key: [REDACTED]",
        );
        assert_redacts(
            "{\"client_secret\": \"s3cr3t-value-9\", \"user\": \"alice\"}",
            "{\"client_secret\": \"[REDACTED]\", \"user\": \"alice\"}",
        );
        assert_redacts(
            "//registry/:_authToken=a1b2c3d4e5f6",
            "//registry/:_authToken=[REDACTED]",
        );
        assert_redacts(
            "Server=db;Password=P@ssw0rd1;Timeout=30",
            "Server=db;Password=[REDACTED];Timeout=30",
        );
    }

    #[test]
    fn secret_assignment_needs_the_keyword_at_the_end_of_the_key() {
        assert_untouched("password_min_length: 12345678");
        assert_untouched("token_expiry: 3600s-or-so");
        assert_untouched("passwordPolicy: strong1234");
        assert_untouched("tokenizer: tokenizer-v2.json");
        assert_untouched("api_key_header: \"X-Api-Key-1\"");
    }

    #[test]
    fn secret_assignment_skips_references_and_placeholders() {
        assert_untouched("secret: ${DB_SECRET_NAME_1}");
        assert_untouched("password: <your password here 1>");
        assert_untouched("password: \"[REDACTED]\"");
        assert_untouched("secret: &SlackApiToken1");
        assert_untouched("password = request.form[\"password\"]");
        assert_untouched("token = get_token(user1)");
    }

    #[test]
    fn secret_assignment_skips_values_without_a_digit_or_symbol() {
        assert_untouched("password: changeme");
        assert_untouched("token: required");
        assert_untouched("self.password = password");
        assert_untouched("secret = settings.SECRET_VALUE");
        assert_untouched("const apiKey = process.env.API_KEY;");
        assert_untouched("def run(password: str, token: Optional[str]) -> Token:");
        assert_redacts("token: required1", "token: [REDACTED]");
        assert_untouched("password: change.me");
        assert_redacts("password: change-me!", "password: [REDACTED]");
    }

    #[test]
    fn quoted_secret_assignment_redacts_up_to_the_closing_quote() {
        assert_redacts(
            "password = \"supersecret,1234\"",
            "password = \"[REDACTED]\"",
        );
        assert_redacts("password = 'super secret; 1234'", "password = '[REDACTED]'");
        assert_redacts(
            "password = `pass, with; delimiters 1`",
            "password = `[REDACTED]`",
        );
        assert_redacts(
            "{\"api_key\": \"ab1,cd2 ef3;gh4\", \"user\": \"alice\"}",
            "{\"api_key\": \"[REDACTED]\", \"user\": \"alice\"}",
        );
        assert_redacts(
            "const token = \"process.env.TOKEN_V2\";",
            "const token = \"[REDACTED]\";",
        );
        assert_untouched("password = \"changeme please\"");
        assert_untouched("password = \"${DB_PASSWORD_1}\"");
        assert_untouched("password = \"[REDACTED]\"");
    }

    #[test]
    fn adjacent_secret_assignments_are_each_redacted() {
        assert_redacts(
            "password=Sup3rS3cret1\npassword=An0therS3cret2\npassword=Th1rdS3cret3",
            "password=[REDACTED]\npassword=[REDACTED]\npassword=[REDACTED]",
        );
        assert_redacts(
            r#"password = "Sup3rS3cret1" password = "An0therS3cret2""#,
            r#"password = "[REDACTED]" password = "[REDACTED]""#,
        );
        assert_redacts(
            "Authorization: Bearer abc.def.ghi\nAuthorization: Bearer jkl.mno.pqr",
            "Authorization: Bearer [REDACTED]\nAuthorization: Bearer [REDACTED]",
        );
    }

    #[test]
    fn secret_rule_cases() {
        let google_oauth = format!("ya29.{}", "a0AfH6SMBxq1w2e3r4t5q1w2e3r4t5q1w2e3r4t5");
        let slack_trigger = format!(
            "hooks.slack.com/triggers/{}",
            "T0123456789/3141592653589/0123456789abcdef0123456789abcdef"
        );
        let slack_workflow = format!(
            "hooks.slack.com/workflows/{}",
            "T0123456789/A0123456789/123456789012345678/abcdefghijklmnopqrstuvwx"
        );
        let huggingface = format!("hf_{}", "0123456789abcdefghijklmnopqrstuv");
        let sendgrid = format!(
            "SG.{}.{}-",
            "abcdefghijklmnopqrstuv", "abcdefghijklmnopqrstuvwxyz0123456789ABCDEF"
        );
        let google_oauth_client = format!("GOCSPX-{}", "x7Kq2mP9vL4tR8wZ3nB6yH1cD5fG");
        let secret_key_base = format!(
            "{}{}",
            "9f3a7c1e5b8d2046a1c9e7f30b5d8a24", "c6e1f9037ab4d85e2c07f1936ab5d48e"
        );

        let discord = format!(
            "{}.{}.{}",
            "MTk4NjIyNDgzNDcxOTI1MjQ4", "Cl2FMQ", "ZnCjm1XVW7vRze4b7Cq4se7kKWs"
        );

        let redacted: Vec<(String, &str)> = vec![
            // A quoted value ends at the next quote of its own kind and keeps the others.
            (
                r#"password = "Sup3rS3cret'sTail""#.into(),
                r#"password = "[REDACTED]""#,
            ),
            (
                r#"password = 'Sup3rS3cret"sTail'"#.into(),
                r#"password = '[REDACTED]'"#,
            ),
            (
                "password = `Sup3rS3cret'\"sTail`".into(),
                "password = `[REDACTED]`",
            ),
            (
                r#"password = "'Sup3rS3cret'""#.into(),
                r#"password = "[REDACTED]""#,
            ),
            (
                r#"password = "supersecret,1234""#.into(),
                r#"password = "[REDACTED]""#,
            ),
            // Escaped quotes, backslashes and doubled quotes stay inside a quoted value.
            (
                r#"{"password": "Sup3rS3cret\"!xtail"}"#.into(),
                r#"{"password": "[REDACTED]"}"#,
            ),
            (
                r#"{"password": "p@ss\"word123"}"#.into(),
                r#"{"password": "[REDACTED]"}"#,
            ),
            (
                r"password = 'it\'s-Sup3r1'".into(),
                "password = '[REDACTED]'",
            ),
            (
                r#"password = "Sup3rS3cret\\" # next"#.into(),
                r#"password = "[REDACTED]" # next"#,
            ),
            (r"PASSWORD='Sup3rS3cret1\'".into(), "PASSWORD='[REDACTED]'"),
            (
                "password: 'it''s-Sup3rS3cret1'".into(),
                "password: '[REDACTED]'",
            ),
            (
                "password: 'Sup3rS3cr''et-tail'".into(),
                "password: '[REDACTED]'",
            ),
            // JSON inside a JSON string.
            (
                r#"{"msg":"{\"password\":\"Sup3rS3cret1\"}"}"#.into(),
                r#"{"msg":"{\"password\":\"[REDACTED]\"}"}"#,
            ),
            // An unquoted value ends at a closing bracket as well.
            (
                "User(name=bob, password=Sup3rS3cret1)".into(),
                "User(name=bob, password=[REDACTED])",
            ),
            (
                "{user=bob, password=Sup3rS3cret1}".into(),
                "{user=bob, password=[REDACTED]}",
            ),
            (
                "{User:bob Password:Sup3rS3cret1}".into(),
                "{User:bob Password:[REDACTED]}",
            ),
            (
                "<td>Password: Sup3rS3cret1</td>".into(),
                "<td>Password: [REDACTED]</td>",
            ),
            // An unquoted value keeps an `&` that does not start a `name=` pair.
            (
                "https://example.com/api?token=abc123def456&user=alice&page=2".into(),
                "https://example.com/api?token=[REDACTED]&user=alice&page=2",
            ),
            (
                "https://example.com/api?user=alice&api_key=abc123def456&page=2".into(),
                "https://example.com/api?user=alice&api_key=[REDACTED]&page=2",
            ),
            ("password: P@ss&word1".into(), "password: [REDACTED]"),
            // Paths are over-redacted rather than risk a secret that starts with `/`.
            (
                "client_secret=/aB3dEfGhIjKlMnOpQrStUv+wXyZ0123456789=".into(),
                "client_secret=[REDACTED]",
            ),
            ("PWD=/home/runner/work/app2".into(), "PWD=[REDACTED]"),
            (
                "secret: /run/secrets/db_password1".into(),
                "secret: [REDACTED]",
            ),
            // Dotted random tokens.
            (
                format!("Google token: {google_oauth}"),
                "Google token: [REDACTED]",
            ),
            (
                "token: abcdefgh1234.ijklmnop5678".into(),
                "token: [REDACTED]",
            ),
            // Connection strings.
            (
                "Driver={ODBC Driver 17};Uid=sa;Pwd=Sup3rS3cret1;".into(),
                "Driver={ODBC Driver 17};Uid=sa;Pwd=[REDACTED];",
            ),
            ("UID=sa;PWD=Sup3rS3cret1".into(), "UID=sa;PWD=[REDACTED]"),
            ("pwd = \"Sup3rS3cret1\"".into(), "pwd = \"[REDACTED]\""),
            (
                "Server=tcp:db;Pwd={Sup3r;S3cret1};Encrypt=yes".into(),
                "Server=tcp:db;Pwd={[REDACTED]};Encrypt=yes",
            ),
            // Vendor tokens.
            (
                format!("POST https://{slack_trigger} done"),
                "POST https://[REDACTED] done",
            ),
            (
                format!("POST https://{slack_workflow} done"),
                "POST https://[REDACTED] done",
            ),
            (format!("HF {huggingface} end"), "HF [REDACTED] end"),
            (format!("key {sendgrid} end"), "key [REDACTED] end"),
            (format!("key {discord} end"), "key [REDACTED] end"),
            // A high-entropy value after a key name no hand-written rule knows.
            (
                format!("google_auth = \"{google_oauth_client}\""),
                r#"google_auth = "[REDACTED]""#,
            ),
            (
                format!("SECRET_KEY_BASE={secret_key_base}"),
                "SECRET_KEY_BASE=[REDACTED]",
            ),
        ];

        let kept = [
            "use crate::signing::private_key::Pkcs8SigningKey;",
            "let key = self::token::Token2::new();",
            "const apiKey = process.env.API_KEY_V2;",
            "apiKey = config.apiKeyV2;",
            "token: process.env.TOKEN_V2",
            "secret = settings::SECRET_VALUE_2;",
            "${{ secrets.X }}",
            r#""{{ vault_password }}""#,
            "${DB_PASSWORD}",
            "var.db_password",
            "password: {required: true, minLength: 8}",
            r#"println!("password={}", password)"#,
            r#"println!("token={token}")"#,
            // `generic-api-key` keeps a value at the entropy floor, one with a stopword and one of
            // letters only.
            r#"creds = "q7m3v9t2hkpshkps""#,
            r#"auth_value = "x9Kq2proc7Zw4mT""#,
            r#"api_token = "ReallyLongLettersOnlyValue""#,
        ];
        let mut mismatches: Vec<String> = redacted
            .iter()
            .map(|(input, expected)| (input.as_str(), *expected))
            .chain(kept.iter().map(|input| (*input, *input)))
            .filter_map(|(input, expected)| {
                let actual = redact_all(input);
                (actual != expected)
                    .then(|| format!("{input}\n    expected: {expected}\n    actual:   {actual}"))
            })
            .collect();
        mismatches.sort();
        assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
    }

    #[test]
    fn vendor_tokens() {
        let openai_project = format!("sk-proj-{}", "abcdefghij0123456789ABCDEFGHIJ");
        let openai_legacy = format!(
            "sk-{}T3BlbkFJ{}",
            "abcdefghij0123456789", "ABCDEFGHIJ0123456789"
        );
        let anthropic = format!("sk-ant-api03-{}", "abcdefghij0123456789ABCDEFGHIJ-_");
        let gitlab = format!("glpat-{}", "abcdefghij0123456789");
        let npm = format!("npm_{}", "abcdefghijklmnopqrstuvwxyz0123456789");
        let slack_webhook = format!(
            "hooks.slack.com/services/{}",
            "T0000AAAA/B0000BBBB/abcdEFGHijklMNOPqrstUVWX"
        );
        let sendgrid = format!(
            "SG.{}.{}",
            "abcdefghijklmnopqrstuv", "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFG"
        );
        let huggingface = format!("hf_{}", "abcdefghijklmnopqrstuvwxyzABCDEFGH");
        let discord = format!(
            "{}.{}.{}",
            "MTk4NjIyNDgzNDcxOTI1MjQ4", "Cl2FMQ", "ZnCjm1XVW7vRze4b7Cq4se7kKWs"
        );
        for token in [
            &openai_project,
            &openai_legacy,
            &anthropic,
            &gitlab,
            &npm,
            &slack_webhook,
            &sendgrid,
            &huggingface,
            &discord,
        ] {
            assert_redacts(&format!("key {token} end"), "key [REDACTED] end");
        }
    }

    #[test]
    fn vendor_tokens_need_their_full_shape() {
        assert_untouched("pip install sk-learn");
        assert_untouched("sk-proj-short");
        assert_untouched("sk-ant-api03-short");
        assert_untouched("glpat-short");
        assert_untouched(&format!("npm_{}", "abcdefghijklmnopqrstuvwxyz012345678"));
        assert_untouched("npm_install_dependencies");
        assert_untouched("hooks.slack.com/services/T0000AAAA/B0000BBBB");
        assert_untouched("SG.short.short");
        assert_untouched("MTk4NjIyNDgzNDcxOTI1MjQ4.Cl2FMQ.short");
        assert_untouched("hf_hub_download(repo_id)");
        assert_untouched(&format!("hf_{}", "abcdefghijklmnopqrstuvwxyzABC"));
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
        let (redacted, findings) = redact(all_rules(), "GB82WEST12345698765432");
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
        assert_eq!(redact(&rules, "a@b.io").0, "a@b.io");
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
            capture_group: 0,
            whole_word: None,
            validator: None,
            examples: Vec::new(),
            counter_examples: Vec::new(),
            category: Category::Other,
        };
        let rules = RuleSet::with_user_rules(&BTreeSet::new(), &[rule]).unwrap();
        let (redacted, findings) = redact(&rules, "badge EMP-123456 issued");
        assert_eq!(redacted, "badge [REDACTED] issued");
        assert_eq!(findings[0].rule.as_str(), "employee-id");
    }

    #[test]
    fn user_dictionary_rule_matches_whole_words_case_insensitively() {
        let rule = UserRule {
            name: RuleName::new("codenames"),
            matcher: UserMatcher::Dictionary(vec!["Bluebird".to_string(), "Kestrel".to_string()]),
            case_insensitive: true,
            capture_group: 0,
            whole_word: None,
            validator: None,
            examples: Vec::new(),
            counter_examples: Vec::new(),
            category: Category::Other,
        };
        let rules = RuleSet::with_user_rules(&BTreeSet::new(), &[rule]).unwrap();
        assert_eq!(
            redact(&rules, "project BLUEBIRD and kestrel").0,
            "project [REDACTED] and [REDACTED]"
        );
        assert_eq!(redact(&rules, "bluebirds fly").0, "bluebirds fly");
    }

    #[test]
    fn user_dictionary_words_are_escaped() {
        let rule = UserRule {
            name: RuleName::new("special"),
            matcher: UserMatcher::Dictionary(vec!["a.b".to_string()]),
            case_insensitive: false,
            capture_group: 0,
            whole_word: None,
            validator: None,
            examples: Vec::new(),
            counter_examples: Vec::new(),
            category: Category::Other,
        };
        let rules = RuleSet::with_user_rules(&BTreeSet::new(), &[rule]).unwrap();
        assert_eq!(redact(&rules, "a.b axb").0, "[REDACTED] axb");
    }

    #[test]
    fn user_dictionary_matches_a_word_with_punctuation_at_its_edges() {
        let rule = UserRule {
            name: RuleName::new("hashtags"),
            matcher: UserMatcher::Dictionary(vec!["#tag".to_string()]),
            case_insensitive: false,
            capture_group: 0,
            whole_word: None,
            validator: None,
            examples: Vec::new(),
            counter_examples: Vec::new(),
            category: Category::Other,
        };
        let rules = RuleSet::with_user_rules(&BTreeSet::new(), &[rule]).unwrap();
        assert_eq!(
            redact(&rules, "wear a #tag today").0,
            "wear a [REDACTED] today"
        );
    }

    #[test]
    fn user_dictionary_matches_a_word_ending_in_punctuation() {
        let rule = UserRule {
            name: RuleName::new("langs"),
            matcher: UserMatcher::Dictionary(vec!["C++".to_string()]),
            case_insensitive: false,
            capture_group: 0,
            whole_word: None,
            validator: None,
            examples: Vec::new(),
            counter_examples: Vec::new(),
            category: Category::Other,
        };
        let rules = RuleSet::with_user_rules(&BTreeSet::new(), &[rule]).unwrap();
        assert_eq!(
            redact(&rules, "we use C++ here").0,
            "we use [REDACTED] here"
        );
    }

    #[test]
    fn user_dictionary_redacts_repeated_adjacent_matches() {
        let rule = UserRule {
            name: RuleName::new("hashtags"),
            matcher: UserMatcher::Dictionary(vec!["#tag".to_string()]),
            case_insensitive: false,
            capture_group: 0,
            whole_word: None,
            validator: None,
            examples: Vec::new(),
            counter_examples: Vec::new(),
            category: Category::Other,
        };
        let rules = RuleSet::with_user_rules(&BTreeSet::new(), &[rule]).unwrap();
        assert_eq!(redact(&rules, "#tag #tag").0, "[REDACTED] [REDACTED]");
    }

    #[test]
    fn user_dictionary_still_requires_a_whole_word_for_plain_words() {
        let rule = UserRule {
            name: RuleName::new("tag"),
            matcher: UserMatcher::Dictionary(vec!["tag".to_string()]),
            case_insensitive: false,
            capture_group: 0,
            whole_word: None,
            validator: None,
            examples: Vec::new(),
            counter_examples: Vec::new(),
            category: Category::Other,
        };
        let rules = RuleSet::with_user_rules(&BTreeSet::new(), &[rule]).unwrap();
        assert_eq!(redact(&rules, "hashtags").0, "hashtags");
        assert_eq!(redact(&rules, "tag_line").0, "tag_line");
    }

    #[test]
    fn user_dictionary_word_followed_by_a_combining_mark_is_not_a_whole_word() {
        let rule = UserRule {
            name: RuleName::new("cafe"),
            matcher: UserMatcher::Dictionary(vec!["cafe".to_string()]),
            case_insensitive: false,
            capture_group: 0,
            whole_word: None,
            validator: None,
            examples: Vec::new(),
            counter_examples: Vec::new(),
            category: Category::Other,
        };
        let rules = RuleSet::with_user_rules(&BTreeSet::new(), &[rule]).unwrap();
        let decomposed = "caf\u{65}\u{301}";
        let (redacted, findings) = redact(&rules, decomposed);
        assert_eq!(redacted, decomposed);
        assert!(findings.is_empty(), "{findings:?}");
        assert_eq!(
            redact(&rules, "a cafe, a caf\u{e9}").0,
            "a [REDACTED], a caf\u{e9}"
        );
    }

    fn dictionary_rule_set(word: &str) -> RuleSet {
        rule_set_from_file(&format!(
            r#"{{"rules":[{{"name":"words","dictionary":["{word}"]}}]}}"#
        ))
        .unwrap()
    }

    #[test]
    fn user_dictionary_word_after_a_mark_that_follows_a_non_word_character_is_a_whole_word() {
        let rules = dictionary_rule_set("Bluebird");
        assert_eq!(
            redact(&rules, "\u{26a0}\u{fe0f}Bluebird is late").0,
            "\u{26a0}\u{fe0f}[REDACTED] is late"
        );
        assert_eq!(redact(&rules, "\u{301}Bluebird").0, "\u{301}[REDACTED]");
    }

    #[test]
    fn user_dictionary_word_is_not_matched_inside_a_word_joined_by_a_zero_width_non_joiner() {
        let rules = dictionary_rule_set("\u{645}\u{6cc}");
        let verb = "\u{645}\u{6cc}\u{200c}\u{62e}\u{648}\u{627}\u{647}\u{645}";
        assert_eq!(redact(&rules, verb).0, verb);
    }

    #[test]
    fn user_dictionary_prefers_the_longest_alternative() {
        let rule = UserRule {
            name: RuleName::new("hashtags"),
            matcher: UserMatcher::Dictionary(vec!["#tag".to_string(), "#tagline".to_string()]),
            case_insensitive: false,
            capture_group: 0,
            whole_word: None,
            validator: None,
            examples: Vec::new(),
            counter_examples: Vec::new(),
            category: Category::Other,
        };
        let rules = RuleSet::with_user_rules(&BTreeSet::new(), &[rule]).unwrap();
        assert_eq!(redact(&rules, "#tagline").0, "[REDACTED]");
    }

    #[test]
    fn user_rule_with_invalid_regex_is_rejected() {
        let rule = UserRule {
            name: RuleName::new("broken"),
            matcher: UserMatcher::Regex("(".to_string()),
            case_insensitive: false,
            capture_group: 0,
            whole_word: None,
            validator: None,
            examples: Vec::new(),
            counter_examples: Vec::new(),
            category: Category::Other,
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
            capture_group: 0,
            whole_word: None,
            validator: None,
            examples: Vec::new(),
            counter_examples: Vec::new(),
            category: Category::Other,
        };
        let rules = RuleSet::with_user_rules(&BTreeSet::new(), &[rule]).unwrap();
        let (redacted, findings) = redact(&rules, "xyz");
        assert_eq!(redacted, "xyz");
        assert!(findings.is_empty(), "{findings:?}");
        assert_eq!(redact(&rules, "aaa xyz").0, "[REDACTED] xyz");
    }

    #[test]
    fn user_dictionary_does_not_match_next_to_a_non_ascii_letter() {
        let word_rule = |word: &str| UserRule {
            name: RuleName::new("tags"),
            matcher: UserMatcher::Dictionary(vec![word.to_string()]),
            case_insensitive: false,
            capture_group: 0,
            whole_word: None,
            validator: None,
            examples: Vec::new(),
            counter_examples: Vec::new(),
            category: Category::Other,
        };
        let rules = RuleSet::with_user_rules(&BTreeSet::new(), &[word_rule("tag")]).unwrap();
        assert_eq!(redact(&rules, "étag").0, "étag");
        assert_eq!(redact(&rules, "tagé").0, "tagé");
        let rules = RuleSet::with_user_rules(&BTreeSet::new(), &[word_rule("étag")]).unwrap();
        assert_eq!(redact(&rules, "étag").0, "[REDACTED]");
    }

    #[test]
    fn two_user_rules_with_the_same_name_are_rejected() {
        let rule = |pattern: &str| UserRule {
            name: RuleName::new("employee-id"),
            matcher: UserMatcher::Regex(pattern.to_string()),
            case_insensitive: false,
            capture_group: 0,
            whole_word: None,
            validator: None,
            examples: Vec::new(),
            counter_examples: Vec::new(),
            category: Category::Other,
        };
        let err = RuleSet::with_user_rules(&BTreeSet::new(), &[rule("a"), rule("b")]).unwrap_err();
        assert!(
            matches!(err, LocalRulesError::DuplicateRule { ref name } if name == "employee-id"),
            "{err}"
        );
    }

    fn rule_set_from_file(contents: &str) -> Result<RuleSet, LocalRulesError> {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), contents).unwrap();
        RuleSet::with_user_rules(&BTreeSet::new(), &load_rules_file(file.path())?)
    }

    #[test]
    fn a_capture_group_redacts_the_group_and_keeps_its_context() {
        let rules = rule_set_from_file(
            r#"{"rules":[{"name":"badge","regex":"badge (EMP-\\d+)","capture_group":1}]}"#,
        )
        .unwrap();
        assert_eq!(
            redact(&rules, "badge EMP-123 issued").0,
            "badge [REDACTED] issued"
        );
    }

    #[test]
    fn a_capture_group_the_regex_lacks_is_rejected() {
        let err = rule_set_from_file(
            r#"{"rules":[{"name":"badge","regex":"badge (EMP-\\d+)","capture_group":2}]}"#,
        )
        .unwrap_err();
        assert!(
            matches!(
                err,
                LocalRulesError::CaptureGroupOutOfRange { ref name, capture_group: 2, groups: 2 }
                    if name == "badge"
            ),
            "{err}"
        );
    }

    #[test]
    fn a_validator_keeps_the_values_that_fail_the_check() {
        let rules = rule_set_from_file(
            r#"{"rules":[{"name":"gift-card","regex":"GC \\d{4} \\d{4} \\d{4} \\d{4}","validator":"payment-card"}]}"#,
        )
        .unwrap();
        assert_eq!(redact(&rules, "GC 4111 1111 1111 1111").0, "[REDACTED]");
        assert_eq!(
            redact(&rules, "GC 4111 1111 1111 1112").0,
            "GC 4111 1111 1111 1112"
        );
    }

    #[test]
    fn a_validator_that_is_not_a_built_in_check_is_rejected() {
        for validator in ["no-such-rule", "email"] {
            let err = rule_set_from_file(&format!(
                r#"{{"rules":[{{"name":"x","regex":"a","validator":"{validator}"}}]}}"#
            ))
            .unwrap_err();
            assert!(
                matches!(
                    err,
                    LocalRulesError::UnknownValidator { ref rule, validator: ref named }
                        if rule == "x" && named == validator
                ),
                "{validator}: {err}"
            );
        }
    }

    #[test]
    fn a_capture_group_rule_makes_one_finding_for_one_match() {
        let rules = rule_set_from_file(
            r#"{"rules":[{"name":"ref","regex":"(\\d{3})\\d*-ID","capture_group":1}]}"#,
        )
        .unwrap();
        let (redacted, findings) = redact(&rules, "ref 123456789-ID end");
        assert_eq!(redacted, "ref [REDACTED]456789-ID end");
        assert_eq!(findings.len(), 1, "{findings:?}");
    }

    #[test]
    fn whole_word_on_a_regex_rule_skips_a_match_inside_a_longer_word() {
        let rules = rule_set_from_file(
            r#"{"rules":[{"name":"ticket","regex":"TK-\\d+","whole_word":true}]}"#,
        )
        .unwrap();
        assert_eq!(redact(&rules, "xTK-12 TK-34").0, "xTK-12 [REDACTED]");
    }

    #[test]
    fn examples_that_hold_let_the_rule_load() {
        rule_set_from_file(
            r#"{"rules":[{"name":"ticket","regex":"TK-\\d+","examples":["see TK-1"],"counter_examples":["TK-","tk-1"]}]}"#,
        )
        .unwrap();
    }

    #[test]
    fn an_example_the_rule_does_not_match_is_rejected() {
        let err = rule_set_from_file(
            r#"{"rules":[{"name":"ticket","regex":"TK-\\d+","examples":["TK-1","TK-"]}]}"#,
        )
        .unwrap_err();
        assert!(
            matches!(
                err,
                LocalRulesError::ExampleNotMatched { ref rule, ref example }
                    if rule == "ticket" && example == "TK-"
            ),
            "{err}"
        );
    }

    #[test]
    fn a_counter_example_the_rule_matches_is_rejected() {
        let err = rule_set_from_file(
            r#"{"rules":[{"name":"ticket","regex":"TK-\\d+","counter_examples":["TK-1"]}]}"#,
        )
        .unwrap_err();
        assert!(
            matches!(
                err,
                LocalRulesError::CounterExampleMatched { ref rule, ref example }
                    if rule == "ticket" && example == "TK-1"
            ),
            "{err}"
        );
    }

    #[test]
    fn user_rule_named_like_a_builtin_is_rejected_whether_or_not_its_group_is_enabled() {
        let rule = || UserRule {
            name: RuleName::new("email"),
            matcher: UserMatcher::Regex("x".to_string()),
            case_insensitive: false,
            capture_group: 0,
            whole_word: None,
            validator: None,
            examples: Vec::new(),
            counter_examples: Vec::new(),
            category: Category::Other,
        };
        for groups in [RuleGroup::all(), BTreeSet::new()] {
            let err = RuleSet::with_user_rules(&groups, &[rule()]).unwrap_err();
            assert!(
                matches!(err, LocalRulesError::BuiltinRuleName { ref name } if name == "email"),
                "{err}"
            );
        }
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

    /// A text with a backtick needs a longer delimiter, padded so the span does not end early.
    fn code_span(text: &str) -> String {
        if text.contains('`') {
            format!("`` {} ``", cell(text))
        } else {
            format!("`{}`", cell(text))
        }
    }

    fn example(example: &Example) -> String {
        match example {
            Example::Published(text, citation) => {
                format!("{} (published, [source]({citation}))", code_span(text))
            }
            Example::Synthetic(text) => format!("{} (synthetic)", code_span(text)),
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
             vendor documentation. `redacter scan -d local-rules` reports the same matches \
             instead of redacting them, labelled with the rule name from the first column of \
             the tables below. Generated from the rule table by \
             `cargo test rules_reference_regenerate -- --ignored`; do not edit by hand.\n\n\
             The pattern and the stopword list of `generic-api-key` are taken from the rule of the \
             same name in [gitleaks](https://github.com/gitleaks/gitleaks), MIT licence, Copyright \
             (c) 2019 Zachary Rice. The licence text is in \
             `src/redacters/local_rules/validators/gitleaks.LICENSE`.\n\n",
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
