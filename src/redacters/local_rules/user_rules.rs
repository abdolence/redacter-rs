use super::error::LocalRulesError;
use super::spans::RuleName;
use serde::Deserialize;
use std::path::Path;

/// A rule supplied by the user, either inline on the command line or from a rule file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserRule {
    pub name: RuleName,
    pub matcher: UserMatcher,
    pub case_insensitive: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UserMatcher {
    Regex(String),
    Dictionary(Vec<String>),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RulesFile {
    rules: Vec<RuleEntry>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RuleEntry {
    name: String,
    regex: Option<String>,
    dictionary: Option<Vec<String>>,
    case_insensitive: Option<bool>,
}

/// Parses a `name=regex` rule given on the command line, splitting on the first `=`.
pub fn parse_inline_rule(value: &str) -> Result<UserRule, LocalRulesError> {
    let invalid = || LocalRulesError::InvalidInlineRule {
        value: value.to_string(),
    };
    let (name, pattern) = value.split_once('=').ok_or_else(invalid)?;
    // A blank name would leave findings unattributable in the report, and a blank pattern
    // matches the empty string everywhere.
    if name.trim().is_empty() || pattern.is_empty() {
        return Err(invalid());
    }
    Ok(UserRule {
        name: RuleName::new(name),
        matcher: UserMatcher::Regex(pattern.to_string()),
        case_insensitive: false,
    })
}

/// Loads user rules from a JSON rule file. Each entry must have exactly one of `regex` or
/// `dictionary`; a dictionary is case-insensitive by default, a regex is not.
pub fn load_rules_file(path: &Path) -> Result<Vec<UserRule>, LocalRulesError> {
    let file_error = |reason: String| LocalRulesError::RuleFile {
        path: path.display().to_string(),
        reason,
    };
    let contents = std::fs::read_to_string(path).map_err(|err| file_error(err.to_string()))?;
    let parsed: RulesFile =
        serde_json::from_str(&contents).map_err(|err| file_error(err.to_string()))?;
    parsed
        .rules
        .into_iter()
        .map(|entry| {
            entry_to_rule(entry).map_err(|reason| match reason {
                EntryError::Matcher(name) => file_error(format!(
                    "rule `{name}` must have exactly one of `regex` or `dictionary`"
                )),
                EntryError::BlankName => file_error("a rule has an empty name".to_string()),
                EntryError::EmptyDictionary(name) => LocalRulesError::EmptyDictionary { name },
                EntryError::BlankDictionaryWord(name) => {
                    LocalRulesError::EmptyDictionaryWord { name }
                }
            })
        })
        .collect()
}

enum EntryError {
    Matcher(String),
    BlankName,
    EmptyDictionary(String),
    BlankDictionaryWord(String),
}

fn entry_to_rule(entry: RuleEntry) -> Result<UserRule, EntryError> {
    // A blank name would leave findings unattributable in the report; a blank dictionary word
    // matches the empty string at every position.
    if entry.name.trim().is_empty() {
        return Err(EntryError::BlankName);
    }
    let (matcher, default_case_insensitive) = match (entry.regex, entry.dictionary) {
        (Some(regex), None) => (UserMatcher::Regex(regex), false),
        (None, Some(words)) if words.is_empty() => {
            return Err(EntryError::EmptyDictionary(entry.name))
        }
        (None, Some(words)) if words.iter().any(|word| word.trim().is_empty()) => {
            return Err(EntryError::BlankDictionaryWord(entry.name))
        }
        (None, Some(words)) => (UserMatcher::Dictionary(words), true),
        _ => return Err(EntryError::Matcher(entry.name)),
    };
    Ok(UserRule {
        name: RuleName::new(&entry.name),
        matcher,
        case_insensitive: entry.case_insensitive.unwrap_or(default_case_insensitive),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_temp(contents: &str) -> tempfile::NamedTempFile {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), contents).unwrap();
        file
    }

    #[test]
    fn inline_rule_splits_on_the_first_equals_sign() {
        let rule = parse_inline_rule("employee-id=EMP-[0-9]{6}=?").unwrap();
        assert_eq!(rule.name.as_str(), "employee-id");
        assert_eq!(
            rule.matcher,
            UserMatcher::Regex("EMP-[0-9]{6}=?".to_string())
        );
        assert!(!rule.case_insensitive);
    }

    #[test]
    fn inline_rule_without_equals_or_with_empty_parts_is_rejected() {
        for value in ["no-equals", "=regex", "name="] {
            assert!(
                matches!(
                    parse_inline_rule(value),
                    Err(LocalRulesError::InvalidInlineRule { .. })
                ),
                "{value}"
            );
        }
    }

    #[test]
    fn rules_file_loads_regex_and_dictionary_rules() {
        let file = write_temp(
            r#"{"rules":[
                {"name":"employee-id","regex":"\\bEMP-[0-9]{6}\\b"},
                {"name":"codenames","dictionary":["Bluebird","Kestrel"]},
                {"name":"loud","dictionary":["SHOUT"],"case_insensitive":false}
            ]}"#,
        );
        let rules = load_rules_file(file.path()).unwrap();
        assert_eq!(rules.len(), 3);
        assert_eq!(
            rules[0].matcher,
            UserMatcher::Regex(r"\bEMP-[0-9]{6}\b".to_string())
        );
        assert!(!rules[0].case_insensitive);
        assert_eq!(
            rules[1].matcher,
            UserMatcher::Dictionary(vec!["Bluebird".to_string(), "Kestrel".to_string()])
        );
        assert!(
            rules[1].case_insensitive,
            "dictionaries are case-insensitive by default"
        );
        assert!(!rules[2].case_insensitive);
    }

    #[test]
    fn rules_file_rejects_both_or_neither_matcher() {
        let both = write_temp(r#"{"rules":[{"name":"x","regex":"a","dictionary":["b"]}]}"#);
        let neither = write_temp(r#"{"rules":[{"name":"x"}]}"#);
        for file in [both, neither] {
            let err = load_rules_file(file.path()).unwrap_err();
            assert!(matches!(err, LocalRulesError::RuleFile { .. }), "{err}");
            assert!(err.to_string().contains("exactly one of"), "{err}");
        }
    }

    #[test]
    fn rules_file_rejects_an_empty_dictionary() {
        let file = write_temp(r#"{"rules":[{"name":"x","dictionary":[]}]}"#);
        assert!(matches!(
            load_rules_file(file.path()).unwrap_err(),
            LocalRulesError::EmptyDictionary { .. }
        ));
    }

    #[test]
    fn inline_rule_with_a_blank_name_is_rejected() {
        for value in [" =a*", "\t=a*"] {
            assert!(
                matches!(
                    parse_inline_rule(value),
                    Err(LocalRulesError::InvalidInlineRule { .. })
                ),
                "{value}"
            );
        }
    }

    #[test]
    fn rules_file_rejects_a_blank_rule_name() {
        for contents in [
            r#"{"rules":[{"name":"","regex":"a"}]}"#,
            r#"{"rules":[{"name":"  ","regex":"a"}]}"#,
        ] {
            let file = write_temp(contents);
            let err = load_rules_file(file.path()).unwrap_err();
            assert!(matches!(err, LocalRulesError::RuleFile { .. }), "{err}");
            assert!(
                err.to_string().contains(&file.path().display().to_string()),
                "{err}"
            );
            assert!(err.to_string().contains("empty name"), "{err}");
        }
    }

    #[test]
    fn rules_file_rejects_a_blank_dictionary_word() {
        let file = write_temp(r#"{"rules":[{"name":"x","dictionary":["ok"," "]}]}"#);
        let err = load_rules_file(file.path()).unwrap_err();
        assert!(
            matches!(err, LocalRulesError::EmptyDictionaryWord { ref name } if name == "x"),
            "{err}"
        );
    }

    #[test]
    fn rules_file_rejects_an_unknown_top_level_key() {
        let file = write_temp(r#"{"rulez":[{"name":"x","regex":"a"}]}"#);
        let err = load_rules_file(file.path()).unwrap_err();
        assert!(matches!(err, LocalRulesError::RuleFile { .. }), "{err}");
        assert!(
            err.to_string().contains(&file.path().display().to_string()),
            "{err}"
        );
        assert!(err.to_string().contains("rulez"), "{err}");
    }

    #[test]
    fn rules_file_reports_missing_file_and_bad_json() {
        let missing = load_rules_file(Path::new("/nonexistent/rules.json")).unwrap_err();
        assert!(matches!(missing, LocalRulesError::RuleFile { .. }));
        let bad = write_temp("{not json");
        assert!(matches!(
            load_rules_file(bad.path()).unwrap_err(),
            LocalRulesError::RuleFile { .. }
        ));
    }
}
