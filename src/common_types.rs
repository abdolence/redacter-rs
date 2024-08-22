use crate::redacters::RedacterThrottler;
use rvstruct::ValueStruct;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(Debug, Clone, ValueStruct)]
pub struct GcpProjectId(String);

#[derive(Debug, Clone, ValueStruct)]
pub struct GcpRegion(String);

#[derive(Debug, Clone, ValueStruct)]
pub struct AwsAccountId(String);

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TextImageCoords {
    pub x1: f32,
    pub y1: f32,
    pub x2: f32,
    pub y2: f32,
    pub text: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DlpRequestLimit {
    pub value: usize,
    pub per: std::time::Duration,
}

impl DlpRequestLimit {
    pub fn new(value: usize, per: std::time::Duration) -> Self {
        assert!(value > 0, "Limit value should be more than zero");
        assert!(
            per.as_millis() > 0,
            "Limit duration should be more than zero"
        );

        Self { value, per }
    }

    pub fn to_rate_limit_in_ms(&self) -> u64 {
        self.per.as_millis() as u64 / self.value as u64
    }

    pub fn to_rate_limit_capacity(&self) -> usize {
        self.per.as_millis() as usize / self.to_rate_limit_in_ms() as usize
    }

    pub fn to_throttling_counter(&self) -> RedacterThrottler {
        RedacterThrottler::new(self.to_rate_limit_capacity(), self.to_rate_limit_in_ms())
    }
}

impl FromStr for DlpRequestLimit {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let index = s.find(|c: char| !c.is_numeric()).unwrap_or(s.len());
        let (number, unit) = s.split_at(index);
        let max_ops_in_units = number
            .parse::<usize>()
            .map_err(|e| format!("Failed to parse number in DlpRequestLimit: {}", e))?;
        println!("max_ops_in_units: {}", max_ops_in_units);
        match unit {
            "rps" => Ok(DlpRequestLimit::new(
                max_ops_in_units,
                std::time::Duration::from_secs(1),
            )),
            "rpm" => Ok(DlpRequestLimit::new(
                max_ops_in_units,
                std::time::Duration::from_secs(60),
            )),
            unknown => Err(format!("Unknown unit specified: {}", unknown)),
        }
    }
}
