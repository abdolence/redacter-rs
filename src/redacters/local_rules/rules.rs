use super::error::LocalRulesError;
use super::spans::{apply_redaction, merge_findings, Finding, RuleName, REDACTED};
use super::user_rules::{UserMatcher, UserRule};
use super::validators::{self, Validator};
use clap::ValueEnum;
use regex::{Regex, RegexBuilder};
use std::collections::BTreeSet;
use std::fmt::Display;

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
    Custom,
}

impl RuleGroup {
    pub fn all() -> BTreeSet<RuleGroup> {
        RuleGroup::value_variants().iter().copied().collect()
    }

    pub fn valid_names() -> String {
        RuleGroup::value_variants()
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ")
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
/// the word "passport" that must survive).
pub struct RuleSpec {
    pub name: &'static str,
    pub group: RuleGroup,
    pub pattern: &'static str,
    pub capture_group: usize,
    pub validator: Option<Validator>,
}

#[derive(Debug)]
struct CompiledRule {
    name: RuleName,
    regex: Regex,
    capture_group: usize,
    validator: Option<Validator>,
}

#[derive(Debug)]
pub struct RuleSet {
    rules: Vec<CompiledRule>,
}

/// Upper bound on the compiled size of one regex, built-in or user-supplied.
pub const REGEX_SIZE_LIMIT: usize = 1 << 20;

const RULES: &[RuleSpec] = &[
    RuleSpec {
        name: "email",
        group: RuleGroup::Email,
        capture_group: 0,
        validator: None,
        pattern: r"(?i)\b[a-z0-9._%+-]+@(?:[a-z0-9-]+\.)+[a-z]{2,}\b",
    },
    RuleSpec {
        name: "phone-international",
        group: RuleGroup::Phone,
        capture_group: 0,
        validator: Some(validators::phone_digits),
        pattern: r"(?:\+|\b00)[1-9]\d{0,2}[ .-]?(?:\(\d{1,4}\)[ .-]?)?\d(?:[ .-]?\d){6,12}\b",
    },
    RuleSpec {
        name: "phone-national",
        group: RuleGroup::Phone,
        capture_group: 0,
        validator: Some(validators::phone_digits),
        pattern: r"(?:\(\d{2,4}\)|\b\d{2,4})[ .-]\d{3}[ .-]\d{3,4}\b",
    },
    RuleSpec {
        name: "iban",
        group: RuleGroup::Iban,
        capture_group: 0,
        validator: Some(validators::iban),
        pattern: r"\b[A-Z]{2}\d{2}(?: ?[A-Z0-9]){11,30}\b",
    },
    RuleSpec {
        name: "payment-card",
        group: RuleGroup::PaymentCard,
        capture_group: 0,
        validator: Some(validators::luhn),
        pattern: r"\b\d(?:[ -]?\d){12,18}\b",
    },
    RuleSpec {
        name: "ipv4",
        group: RuleGroup::Network,
        capture_group: 0,
        validator: Some(validators::ipv4),
        pattern: r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b",
    },
    RuleSpec {
        name: "ipv6",
        group: RuleGroup::Network,
        capture_group: 0,
        validator: Some(validators::ipv6),
        pattern: r"(?i)\b(?:[0-9a-f]{0,4}:){2,7}[0-9a-f]{1,4}\b",
    },
    RuleSpec {
        name: "mac-address",
        group: RuleGroup::Network,
        capture_group: 0,
        validator: None,
        pattern: r"(?i)\b(?:[0-9a-f]{2}[:-]){5}[0-9a-f]{2}\b",
    },
    RuleSpec {
        name: "url-credentials",
        group: RuleGroup::Url,
        capture_group: 1,
        validator: None,
        pattern: r"\b[a-zA-Z][a-zA-Z0-9+.-]*://([^\s/@:]+:[^\s/@]+)@",
    },
    RuleSpec {
        name: "aws-access-key",
        group: RuleGroup::Secrets,
        capture_group: 0,
        validator: None,
        pattern: r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b",
    },
    RuleSpec {
        name: "aws-secret-key",
        group: RuleGroup::Secrets,
        capture_group: 1,
        validator: None,
        pattern: r#"(?i)aws.{0,20}?secret.{0,20}?[=:'"\s]\s*([A-Za-z0-9/+=]{40})(?:[^A-Za-z0-9/+=]|$)"#,
    },
    RuleSpec {
        name: "gcp-api-key",
        group: RuleGroup::Secrets,
        capture_group: 1,
        validator: None,
        pattern: r"\b(AIza[0-9A-Za-z_-]{35})(?:[^0-9A-Za-z_-]|$)",
    },
    RuleSpec {
        name: "github-token",
        group: RuleGroup::Secrets,
        capture_group: 0,
        validator: None,
        pattern: r"\b(?:(?:ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9]{36,}|github_pat_[A-Za-z0-9_]{22,})\b",
    },
    RuleSpec {
        name: "slack-token",
        group: RuleGroup::Secrets,
        capture_group: 0,
        validator: None,
        pattern: r"\bxox[abpr]-[A-Za-z0-9-]{10,}\b",
    },
    RuleSpec {
        name: "stripe-key",
        group: RuleGroup::Secrets,
        capture_group: 0,
        validator: None,
        pattern: r"\b(?:sk|rk)_live_[A-Za-z0-9]{16,}\b",
    },
    RuleSpec {
        name: "jwt",
        group: RuleGroup::Secrets,
        capture_group: 0,
        validator: None,
        pattern: r"\beyJ[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]{5,}\b",
    },
    RuleSpec {
        name: "authorization-header",
        group: RuleGroup::Secrets,
        capture_group: 1,
        validator: None,
        pattern: r"(?i)\bauthorization\s*:\s*(?:bearer|basic)\s+([A-Za-z0-9._~+/=-]+)",
    },
    RuleSpec {
        name: "private-key-block",
        group: RuleGroup::Secrets,
        capture_group: 0,
        validator: None,
        pattern: r"(?s)-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----",
    },
    RuleSpec {
        name: "us-ssn",
        group: RuleGroup::UsIdentifiers,
        capture_group: 0,
        validator: Some(validators::us_ssn_or_itin),
        pattern: r"\b\d{3}-\d{2}-\d{4}\b",
    },
    RuleSpec {
        name: "us-passport",
        group: RuleGroup::UsIdentifiers,
        capture_group: 1,
        validator: None,
        pattern: r"(?i)\bpassport\b.{0,20}?\b([A-Z]?\d{8,9})\b",
    },
    RuleSpec {
        name: "eu-vat",
        group: RuleGroup::EuIdentifiers,
        capture_group: 0,
        validator: Some(validators::eu_vat),
        pattern: r"\b(?:DE\d{9}|FR[A-Z0-9]{2}\d{9}|IT\d{11}|ES[A-Z0-9]\d{7}[A-Z0-9]|NL\d{9}B\d{2}|BE0\d{9}|GB\d{9}(?:\d{3})?)\b",
    },
    RuleSpec {
        name: "spanish-dni-nie",
        group: RuleGroup::EuIdentifiers,
        capture_group: 0,
        validator: Some(validators::spanish_dni_nie),
        pattern: r"\b(?:\d{8}|[XYZ]\d{7})[A-Z]\b",
    },
    RuleSpec {
        name: "italian-codice-fiscale",
        group: RuleGroup::EuIdentifiers,
        capture_group: 0,
        validator: Some(validators::italian_codice_fiscale),
        pattern: r"\b[A-Z]{6}\d{2}[A-EHLMPRST]\d{2}[A-Z]\d{3}[A-Z]\b",
    },
    RuleSpec {
        name: "dutch-bsn",
        group: RuleGroup::EuIdentifiers,
        capture_group: 1,
        validator: Some(validators::dutch_bsn),
        pattern: r"(?i)\b(?:bsn|burgerservicenummer|sofinummer|sofi-nummer)\b.{0,20}?\b(\d{9})\b",
    },
    RuleSpec {
        name: "uk-nino",
        group: RuleGroup::EuIdentifiers,
        capture_group: 0,
        validator: Some(validators::uk_nino),
        pattern: r"\b[A-Z]{2} ?\d{2} ?\d{2} ?\d{2} ?[A-D]\b",
    },
    RuleSpec {
        name: "french-nir",
        group: RuleGroup::EuIdentifiers,
        capture_group: 0,
        validator: Some(validators::french_nir),
        pattern: r"\b[12] ?\d{2} ?(?:0[1-9]|1[0-2]|20) ?(?:\d{2}|2[AB]) ?\d{3} ?\d{3} ?\d{2}\b",
    },
    RuleSpec {
        name: "german-steuer-id",
        group: RuleGroup::EuIdentifiers,
        capture_group: 1,
        validator: Some(validators::german_steuer_id),
        pattern: r"(?i)\b(?:steuer-?id|steuer-?idnr|idnr|steueridentifikationsnummer|steuerliche identifikationsnummer|tax id)\b.{0,20}?\b([1-9]\d{10})\b",
    },
];

pub fn builtin_rules() -> &'static [RuleSpec] {
    RULES
}

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
                validator: spec.validator,
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
            let pattern = match &rule.matcher {
                UserMatcher::Regex(pattern) => pattern.clone(),
                UserMatcher::Dictionary(words) => {
                    if words.is_empty() {
                        return Err(LocalRulesError::EmptyDictionary {
                            name: rule.name.to_string(),
                        });
                    }
                    let escaped: Vec<String> = words.iter().map(|w| regex::escape(w)).collect();
                    format!(r"\b(?:{})\b", escaped.join("|"))
                }
            };
            set.rules.push(CompiledRule {
                name: rule.name.clone(),
                regex: compile(rule.name.as_str(), &pattern, rule.case_insensitive)?,
                capture_group: 0,
                validator: None,
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
                if rule
                    .validator
                    .is_some_and(|validate| !validate(matched.as_str()))
                {
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
mod tests {
    use super::*;

    fn all_rules() -> RuleSet {
        RuleSet::new(&RuleGroup::all()).unwrap()
    }

    fn redact_all(text: &str) -> String {
        all_rules().redact(text).0
    }

    fn assert_redacts(text: &str, expected: &str) {
        assert_eq!(redact_all(text), expected, "input: {text}");
    }

    fn assert_untouched(text: &str) {
        assert_eq!(redact_all(text), text, "input: {text}");
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
