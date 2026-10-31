//! The common category set the benchmark scores recall in, so one number per category is
//! comparable across redacters whatever they call a finding.

use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

/// Displayed under the same snake_case name that `truth.json` and `results.json` use.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Category {
    Person,
    Contact,
    Address,
    IdNumber,
    Financial,
    DateOfBirth,
    Credential,
    Network,
    Other,
}

impl Category {
    pub const ALL: [Category; 9] = [
        Category::Person,
        Category::Contact,
        Category::Address,
        Category::IdNumber,
        Category::Financial,
        Category::DateOfBirth,
        Category::Credential,
        Category::Network,
        Category::Other,
    ];

    /// The category of a label of the public dataset (`nvidia/Nemotron-PII`), `None` for a
    /// label the fixtures treat as neutral: places, organisations, dates and times, and
    /// occupational or educational attributes. Whether a redacter finds one is a configuration
    /// choice, so it counts neither for nor against. Any other label is sensitive.
    pub fn of_nemotron_label(label: &str) -> Option<Category> {
        match label {
            "city" | "county" | "state" | "country" | "company_name" | "date" | "date_time"
            | "time" | "occupation" | "language" | "employment_status" | "education_level" => None,
            "first_name" | "last_name" | "user_name" => Some(Category::Person),
            "email" | "phone_number" | "fax_number" => Some(Category::Contact),
            "street_address" | "postcode" | "coordinate" => Some(Category::Address),
            "ssn"
            | "national_id"
            | "tax_id"
            | "customer_id"
            | "employee_id"
            | "certificate_license_number"
            | "license_plate"
            | "vehicle_identifier"
            | "unique_id"
            | "medical_record_number"
            | "health_plan_beneficiary_number"
            | "device_identifier"
            | "biometric_identifier" => Some(Category::IdNumber),
            "credit_debit_card"
            | "cvv"
            | "bank_routing_number"
            | "account_number"
            | "swift_bic" => Some(Category::Financial),
            "date_of_birth" => Some(Category::DateOfBirth),
            "password" | "api_key" | "http_cookie" | "pin" => Some(Category::Credential),
            "ipv4" | "ipv6" | "url" | "mac_address" => Some(Category::Network),
            _ => Some(Category::Other),
        }
    }
}

impl Display for Category {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Category::Person => "person",
            Category::Contact => "contact",
            Category::Address => "address",
            Category::IdNumber => "id_number",
            Category::Financial => "financial",
            Category::DateOfBirth => "date_of_birth",
            Category::Credential => "credential",
            Category::Network => "network",
            Category::Other => "other",
        })
    }
}
