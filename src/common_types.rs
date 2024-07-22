use rvstruct::ValueStruct;

#[derive(Debug, Clone, ValueStruct)]
pub struct GcpProjectId(String);

#[derive(Debug, Clone, ValueStruct)]
pub struct AwsAccountId(String);
