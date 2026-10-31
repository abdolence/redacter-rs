//! The common category set the benchmark scores recall in, so one number per category is
//! comparable across redacters whatever they call a finding.

use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

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
}

impl Display for Category {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Category::Person => "person",
            Category::Contact => "contact",
            Category::Address => "address",
            Category::IdNumber => "id",
            Category::Financial => "financial",
            Category::DateOfBirth => "dob",
            Category::Credential => "credential",
            Category::Network => "network",
            Category::Other => "other",
        })
    }
}

/// The category of a label of the public dataset (`nvidia/Nemotron-PII`). Labels that name a
/// person's online handle count as a person; everything that is neither an identifier nor a
/// location (a job title, a language, a time of day, a health or belief attribute) is `Other`,
/// which also catches labels a later revision of the dataset adds.
impl From<&str> for Category {
    fn from(label: &str) -> Self {
        match label {
            "first_name" | "last_name" | "user_name" => Category::Person,
            "email" | "phone_number" | "fax_number" => Category::Contact,
            "street_address" | "city" | "county" | "state" | "country" | "postcode"
            | "coordinate" => Category::Address,
            "ssn"
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
            | "biometric_identifier" => Category::IdNumber,
            "credit_debit_card"
            | "cvv"
            | "bank_routing_number"
            | "account_number"
            | "swift_bic" => Category::Financial,
            "date_of_birth" => Category::DateOfBirth,
            "password" | "api_key" | "http_cookie" | "pin" => Category::Credential,
            "ipv4" | "ipv6" | "url" | "mac_address" => Category::Network,
            _ => Category::Other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dataset_labels_map_to_the_common_categories() {
        assert_eq!(Category::from("first_name"), Category::Person);
        assert_eq!(Category::from("phone_number"), Category::Contact);
        assert_eq!(Category::from("postcode"), Category::Address);
        assert_eq!(Category::from("ssn"), Category::IdNumber);
        assert_eq!(Category::from("credit_debit_card"), Category::Financial);
        assert_eq!(Category::from("date_of_birth"), Category::DateOfBirth);
        assert_eq!(Category::from("api_key"), Category::Credential);
        assert_eq!(Category::from("ipv6"), Category::Network);
    }

    #[test]
    fn labels_outside_the_table_are_other() {
        assert_eq!(Category::from("occupation"), Category::Other);
        assert_eq!(Category::from("a_label_added_later"), Category::Other);
    }
}
