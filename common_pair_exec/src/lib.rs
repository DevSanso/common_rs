use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;
use common_core::collection::pool::{PoolItem, ThreadSafePool};
use common_err::CommonError;
#[derive(Clone, Debug, PartialEq)]
pub enum PairValueEnum {
    Double(f64),
    Int(i32),
    BigInt(i64),
    String(String),
    Bin(Vec<u8>),
    Bool(bool),
    Float(f32),
    Array(Vec<PairValueEnum>),
    Map(HashMap<String, PairValueEnum>),
    Null
}

impl Default for PairValueEnum {
    fn default() -> Self {PairValueEnum::Null}
}

#[derive(Debug,Clone, Default)]
pub struct PairExecutorInfo {
    pub addr : String,
    pub name : String,
    pub user : String,
    pub password : String,
    pub timeout_sec : u32
}

#[derive(Clone, Debug, Default)]
pub struct PairExecuteRet(pub String, pub PairValueEnum);

impl Display for PairValueEnum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", match self {
            PairValueEnum::Double(d) => d.to_string(),
            PairValueEnum::Int(i) => i.to_string(),
            PairValueEnum::BigInt(i) => i.to_string(),
            PairValueEnum::String(s) => s.to_string(),
            PairValueEnum::Bool(b) => b.to_string(),
            PairValueEnum::Float(f) => f.to_string(),
            PairValueEnum::Null => "NULL".to_string(),
            PairValueEnum::Bin(b) => String::from_utf8(b.clone()).unwrap(),
            PairValueEnum::Map(m) => format!("{:?}", m),
            PairValueEnum::Array(a) => format!("{:?}", a),
        })
    }
}

pub trait PairExecutor {
    fn execute_pair(&mut self, query : &'_ str, param : &PairExecuteRet) -> Result<PairExecuteRet, CommonError>;
    fn get_current_time(&mut self) -> Result<std::time::Duration, CommonError>;
}

pub type PairExecutorBox = Box<dyn PoolItem<Box<dyn PairExecutor>>>;
pub type PairExecutorPool = Arc<dyn ThreadSafePool<Box<dyn PairExecutor>,()>>;