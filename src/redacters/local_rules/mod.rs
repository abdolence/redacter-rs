mod error;
mod rules;
mod spans;
mod validators;

#[allow(unused_imports)] // consumed by LocalRulesRedacter, wired in Task 5
pub use error::LocalRulesError;
#[allow(unused_imports)] // consumed by LocalRulesRedacter, wired in Task 5
pub use rules::{RuleGroup, RuleSet};
#[allow(unused_imports)] // consumed by LocalRulesRedacter, wired in Task 5
pub use spans::{apply_redaction, merge_findings, Finding, RuleName, REDACTED};
