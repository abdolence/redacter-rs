//! Secret rules ported from gitleaks (`vendor/gitleaks.toml`, MIT). The table in `table.rs`
//! is generated from that file; this module holds what a rule needs at match time: the
//! keyword prefilter, the entropy floor and the allowlists.
//!
//! Gitleaks patterns are RE2, where `\w`, `\s` and `\b` are ASCII only. They compile
//! without Unicode so that those classes keep their meaning and stay small; the few that
//! need Unicode-aware negated classes fall back to the default mode.

use super::super::error::LocalRulesError;
use super::{Strictness, REGEX_SIZE_LIMIT};
use crate::redacters::text_spans::{Finding, RuleName};
use aho_corasick::AhoCorasick;
use regex::{Regex, RegexBuilder};
use std::collections::{BTreeMap, HashMap};
use std::ops::Range;
use std::sync::OnceLock;

#[cfg(test)]
mod generator;
#[rustfmt::skip]
mod table;

use table::{GLOBAL_ALLOWLIST, RULES};

/// What an allowlist regex is matched against.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AllowTarget {
    /// The secret, which is the redacted capture group.
    Secret,
    /// The whole regex match.
    Match,
    /// The line holding the match.
    Line,
}

#[derive(Debug)]
/// Findings an allowlist lets through: those matching one of its regexes on `target`, or
/// whose secret contains one of its stopwords (compared in lower case).
pub(super) struct Allowlist {
    pub target: AllowTarget,
    pub regexes: &'static [&'static str],
    pub stopwords: &'static [&'static str],
}

#[derive(Debug)]
/// One gitleaks rule. `secret_group` 0 is the whole match. `entropy` is the Shannon entropy
/// the secret must reach. `keywords` are lower case: a rule runs only on text that contains
/// one of them, whatever the case.
pub(super) struct GitleaksRule {
    pub id: &'static str,
    // Read only by the rules reference; a binary crate fails `-D warnings` on an unread field.
    #[cfg_attr(not(test), allow(dead_code))]
    pub description: &'static str,
    pub pattern: &'static str,
    pub secret_group: usize,
    pub entropy: Option<f64>,
    pub keywords: &'static [&'static str],
    pub allowlists: &'static [Allowlist],
}

/// `pattern` with every `{` that does not open a repetition escaped. RE2 reads such a brace
/// as a literal (`\$\{\d+}`), and the `regex` crate reports an error.
fn with_literal_braces(pattern: &str) -> String {
    let mut out = String::with_capacity(pattern.len());
    let mut chars = pattern.char_indices();
    while let Some((at, character)) = chars.next() {
        match character {
            '\\' => {
                out.push(character);
                if let Some((_, escaped)) = chars.next() {
                    out.push(escaped);
                }
            }
            '{' if !opens_repetition(&pattern[at + 1..]) => out.push_str("\\{"),
            _ => out.push(character),
        }
    }
    out
}

/// True when `rest` starts like the inside of `{n}`, `{n,}` or `{n,m}`.
fn opens_repetition(rest: &str) -> bool {
    let digits = rest.bytes().take_while(u8::is_ascii_digit).count();
    let after_first = &rest[digits..];
    let after_second = match after_first.strip_prefix(',') {
        Some(tail) => &tail[tail.bytes().take_while(u8::is_ascii_digit).count()..],
        None => after_first,
    };
    digits > 0 && after_second.starts_with('}')
}

/// The ported rules, for the rules reference.
#[cfg(test)]
pub(super) fn ported_rules() -> &'static [GitleaksRule] {
    RULES
}

/// The gitleaks commit the ported rules were taken from.
#[cfg(test)]
pub(super) fn source_commit() -> &'static str {
    table::SOURCE_COMMIT
}

/// The rules of the vendored file that are not ported, each with the reason.
#[cfg(test)]
pub(super) fn not_ported_rules() -> &'static [(&'static str, &'static str)] {
    table::NOT_PORTED
}

/// Compiles a gitleaks pattern or allowlist regex.
pub(super) fn compile_pattern(rule: &str, pattern: &str) -> Result<Regex, LocalRulesError> {
    let pattern = &with_literal_braces(pattern);
    RegexBuilder::new(pattern)
        .unicode(false)
        .size_limit(REGEX_SIZE_LIMIT)
        .build()
        .or_else(|_| {
            RegexBuilder::new(pattern)
                .size_limit(REGEX_SIZE_LIMIT)
                .build()
        })
        .map_err(|err| LocalRulesError::InvalidRegex {
            rule: rule.to_string(),
            reason: err.to_string(),
        })
}

#[derive(Debug)]
struct CompiledAllowlist {
    target: AllowTarget,
    regexes: Vec<Regex>,
    stopwords: &'static [&'static str],
}

impl CompiledAllowlist {
    fn new(rule: &str, allowlist: &Allowlist) -> Result<Self, LocalRulesError> {
        Ok(Self {
            target: allowlist.target,
            regexes: allowlist
                .regexes
                .iter()
                .map(|pattern| compile_pattern(rule, pattern))
                .collect::<Result<_, _>>()?,
            stopwords: allowlist.stopwords,
        })
    }

    fn allows(&self, found: &Found<'_>) -> bool {
        let target = match self.target {
            AllowTarget::Secret => found.secret,
            AllowTarget::Match => found.matched,
            AllowTarget::Line => found.line,
        };
        self.regexes.iter().any(|regex| regex.is_match(target))
            || (!self.stopwords.is_empty() && {
                let lowered = found.secret.to_lowercase();
                self.stopwords.iter().any(|word| lowered.contains(word))
            })
    }
}

/// A candidate to judge, in the three views an allowlist can look at.
struct Found<'a> {
    secret: &'a str,
    matched: &'a str,
    line: &'a str,
}

impl<'a> Found<'a> {
    fn new(text: &'a str, matched: Range<usize>, secret: &'a str) -> Self {
        let line_start = text[..matched.start].rfind('\n').map_or(0, |at| at + 1);
        let line_end = text[matched.end..]
            .find('\n')
            .map_or(text.len(), |at| matched.end + at);
        Self {
            secret,
            matched: &text[matched],
            line: &text[line_start..line_end],
        }
    }
}

/// Shannon entropy of the characters of `text`, in bits per character.
fn shannon_entropy(text: &str) -> f64 {
    let mut counts: BTreeMap<char, u32> = BTreeMap::new();
    let mut total = 0u32;
    for character in text.chars() {
        *counts.entry(character).or_default() += 1;
        total += 1;
    }
    counts
        .values()
        .map(|&count| {
            let probability = f64::from(count) / f64::from(total);
            -probability * probability.log2()
        })
        .sum()
}

/// A rule's regex and rule-level allowlists, compiled on the first text that holds one of
/// its keywords: most runs never meet a vendor's keyword, and the patterns are many.
#[derive(Debug)]
struct Compiled {
    regex: Regex,
    allowlists: Vec<CompiledAllowlist>,
}

impl Compiled {
    fn new(rule: &GitleaksRule) -> Result<Self, LocalRulesError> {
        Ok(Self {
            regex: compile_pattern(rule.id, rule.pattern)?,
            allowlists: rule
                .allowlists
                .iter()
                .map(|allowlist| CompiledAllowlist::new(rule.id, allowlist))
                .collect::<Result<_, _>>()?,
        })
    }
}

#[derive(Debug)]
struct Matcher {
    /// Position of the rule in the table, which is how the keyword index names it.
    position: usize,
    rule: &'static GitleaksRule,
    name: RuleName,
    compiled: OnceLock<Compiled>,
}

impl Matcher {
    fn compiled(&self) -> &Compiled {
        self.compiled.get_or_init(|| {
            Compiled::new(self.rule).expect("the generated table holds only compilable patterns")
        })
    }
}

/// The gitleaks rules of a rule set. Only a strict set applies the entropy floors and the
/// allowlists.
#[derive(Debug)]
pub(super) struct GitleaksRules {
    matchers: Vec<Matcher>,
    keyword_index: KeywordIndex,
    global_allowlist: OnceLock<CompiledAllowlist>,
    strictness: Strictness,
}

impl GitleaksRules {
    pub(super) fn new(strictness: Strictness) -> Result<Self, LocalRulesError> {
        Ok(Self {
            matchers: RULES
                .iter()
                .enumerate()
                .map(|(position, rule)| Matcher {
                    position,
                    rule,
                    name: RuleName::new(rule.id),
                    compiled: OnceLock::new(),
                })
                .collect(),
            keyword_index: KeywordIndex::new(RULES.iter().map(|rule| rule.keywords))?,
            global_allowlist: OnceLock::new(),
            strictness,
        })
    }

    pub(super) fn names(&self) -> impl Iterator<Item = &RuleName> {
        self.matchers.iter().map(|matcher| &matcher.name)
    }

    /// Adds to `findings` every secret of the rules whose keywords `text` holds.
    pub(super) fn find_into(&self, text: &str, findings: &mut Vec<Finding>) {
        let mut keyword_present: Vec<bool> =
            RULES.iter().map(|rule| rule.keywords.is_empty()).collect();
        self.keyword_index.mark_rules_in(text, &mut keyword_present);
        for matcher in self
            .matchers
            .iter()
            .filter(|matcher| keyword_present[matcher.position])
        {
            let compiled = matcher.compiled();
            for captures in compiled.regex.captures_iter(text) {
                let (Some(whole), Some(secret)) =
                    (captures.get(0), captures.get(matcher.rule.secret_group))
                else {
                    continue;
                };
                if secret.is_empty() {
                    continue;
                }
                if self.strictness == Strictness::Strict {
                    let found = Found::new(text, whole.range(), secret.as_str());
                    if !self.accepts(matcher, compiled, &found) {
                        continue;
                    }
                }
                findings.push(Finding {
                    start: secret.start(),
                    end: secret.end(),
                    rule: matcher.name.clone(),
                    score: None,
                });
            }
        }
    }

    fn accepts(&self, matcher: &Matcher, compiled: &Compiled, found: &Found<'_>) -> bool {
        let global = self.global_allowlist.get_or_init(|| {
            CompiledAllowlist::new("global allowlist", &GLOBAL_ALLOWLIST)
                .expect("the generated table holds only compilable patterns")
        });
        matcher
            .rule
            .entropy
            .is_none_or(|floor| shannon_entropy(found.secret) >= floor)
            && !global.allows(found)
            && !compiled
                .allowlists
                .iter()
                .any(|allowlist| allowlist.allows(found))
    }
}

/// Finds, in one pass over a text, which rules have a keyword in it.
#[derive(Debug)]
struct KeywordIndex {
    automaton: AhoCorasick,
    rules_of_keyword: Vec<Vec<usize>>,
}

impl KeywordIndex {
    /// `keywords_of_rule` lists the keywords of each rule, in rule order.
    fn new(
        keywords_of_rule: impl IntoIterator<Item = &'static [&'static str]>,
    ) -> Result<Self, LocalRulesError> {
        let mut keywords: Vec<&'static str> = Vec::new();
        let mut rules_of_keyword: Vec<Vec<usize>> = Vec::new();
        let mut position_of_keyword: HashMap<&'static str, usize> = HashMap::new();
        for (rule, rule_keywords) in keywords_of_rule.into_iter().enumerate() {
            for keyword in rule_keywords {
                let position = *position_of_keyword.entry(keyword).or_insert_with(|| {
                    keywords.push(keyword);
                    rules_of_keyword.push(Vec::new());
                    keywords.len() - 1
                });
                rules_of_keyword[position].push(rule);
            }
        }
        let automaton = AhoCorasick::builder()
            .ascii_case_insensitive(true)
            .build(&keywords)
            .map_err(|err| LocalRulesError::InvalidKeywords {
                reason: err.to_string(),
            })?;
        Ok(Self {
            automaton,
            rules_of_keyword,
        })
    }

    /// Sets `present[rule]` for every rule that has a keyword in `text`.
    fn mark_rules_in(&self, text: &str, present: &mut [bool]) {
        for found in self.automaton.find_overlapping_iter(text) {
            for &rule in &self.rules_of_keyword[found.pattern().as_usize()] {
                present[rule] = true;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entropy_of_repeated_and_varied_text() {
        assert_eq!(shannon_entropy("aaaa"), 0.0);
        assert!((shannon_entropy("abcd") - 2.0).abs() < 1e-9);
    }

    #[test]
    fn keywords_match_in_any_case_and_name_every_rule_sharing_them() {
        let index =
            KeywordIndex::new([&["token"][..], &["token", "ghp_"][..], &["other"][..]]).unwrap();
        let mut present = [false; 3];
        index.mark_rules_in("my GHP_value", &mut present);
        assert_eq!(present, [false, true, false]);
        let mut present = [false; 3];
        index.mark_rules_in("API TOKEN", &mut present);
        assert_eq!(present, [true, true, false]);
    }

    #[test]
    fn every_gitleaks_rule_compiles() {
        for matcher in GitleaksRules::new(Strictness::Strict).unwrap().matchers {
            matcher.compiled();
        }
    }

    #[test]
    fn literal_braces_are_escaped_and_repetitions_are_not() {
        assert_eq!(with_literal_braces(r"^\$\d+|{\d+}$"), r"^\$\d+|\{\d+}$");
        assert_eq!(with_literal_braces("a{2}b{3,}c{4,5}"), "a{2}b{3,}c{4,5}");
    }
}
