use rvstruct::ValueStruct;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, ValueStruct)]
pub struct GcpProjectId(String);

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
