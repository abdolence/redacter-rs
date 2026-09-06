mod error;
mod spans;

#[allow(unused_imports)] // consumed by LocalRulesRedacter, wired in Task 5
pub use error::LocalRulesError;
#[allow(unused_imports)] // consumed by LocalRulesRedacter, wired in Task 5
pub use spans::{apply_redaction, merge_findings, Finding, RuleName, REDACTED};
