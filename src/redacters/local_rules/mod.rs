mod error;
mod rules;
mod spans;
mod user_rules;
mod validators;

#[allow(unused_imports)] // consumed by LocalRulesRedacter, wired in Task 5
pub use error::LocalRulesError;
#[allow(unused_imports)] // consumed by LocalRulesRedacter, wired in Task 5
pub use rules::{RuleGroup, RuleSet};
#[allow(unused_imports)] // consumed by LocalRulesRedacter, wired in Task 5
pub use spans::{apply_redaction, merge_findings, Finding, RuleName, REDACTED};
#[allow(unused_imports)] // consumed by LocalRulesRedacter, wired in Task 5
pub use user_rules::{load_rules_file, parse_inline_rule, UserMatcher, UserRule};
