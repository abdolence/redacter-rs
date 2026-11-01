//! The kinds of sensitive data `--categories` chooses by. This file is also compiled into
//! `tests/bench_dlp` with `#[path]`, so it uses nothing from the rest of the crate and holds
//! no tests.

use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// A kind of sensitive data. Every label of every redacter is in exactly one category.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, ValueEnum, Serialize, Deserialize,
)]
#[serde(rename_all = "kebab-case")]
pub enum Category {
    Person,
    Contact,
    Address,
    Location,
    Organization,
    IdNumber,
    Financial,
    DateOfBirth,
    Secrets,
    Network,
    Other,
}

impl Category {
    /// What the category holds, in the words of the model prompt and the README.
    pub fn description(self) -> &'static str {
        match self {
            Self::Person => "names of people and user names",
            Self::Contact => "email addresses, phone and fax numbers",
            Self::Address => "street addresses, postcodes and geographic coordinates",
            Self::Location => "cities, regions, countries and other named places that are not street addresses",
            Self::Organization => "companies, institutions and employers",
            Self::IdNumber => {
                "national, tax, passport, driving licence, health, customer, employee, vehicle and device identifiers"
            }
            Self::Financial => {
                "payment card numbers and their security codes, bank account and routing numbers, IBAN and SWIFT codes"
            }
            Self::DateOfBirth => "dates of birth",
            Self::Secrets => {
                "passwords, PINs, API keys, access tokens, private keys, session cookies and other secrets"
            }
            Self::Network => "IP addresses, MAC addresses and URLs",
            Self::Other => {
                "any other personal or sensitive information, such as other dates, ages, genders and medical details"
            }
        }
    }
}

impl Display for Category {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.to_possible_value() {
            Some(value) => f.write_str(value.get_name()),
            None => write!(f, "{self:?}"),
        }
    }
}
