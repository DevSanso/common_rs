use std::error::Error;
use std::fmt::Display;
use std::sync::Arc;

use common_core::collection::pool::ThreadSafePool;
use common_core::collection::pool::PoolItem;
use common_err::CommonError;

#[derive(Clone, Debug, PartialEq)]
pub enum RelationalValue {
    Double(f64),
    Int(i32),
    BigInt(i64),
    String(String),
    Bin(Vec<u8>),
    Bool(bool),
    Float(f32),
    Null
}

impl Display for RelationalValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", match self {
            RelationalValue::Double(d) => d.to_string(),
            RelationalValue::Int(i) => i.to_string(),
            RelationalValue::BigInt(i) => i.to_string(),
            RelationalValue::String(s) => s.to_string(),
            RelationalValue::Bool(b) => b.to_string(),
            RelationalValue::Float(f) => f.to_string(),
            RelationalValue::Null => "NULL".to_string(),
            RelationalValue::Bin(b) => String::from_utf8(b.clone()).unwrap(),
        })
    }
}

#[derive(Default,Clone)]
pub struct RelationalExecuteResultSet {
    pub cols_name : Vec<String>,
    pub cols_data : Vec<Vec<RelationalValue>>
}

pub trait RelationalExecutor<PARAM> {
    fn execute(&mut self, query : &'_ str, param : &[PARAM]) -> Result<RelationalExecuteResultSet, CommonError>;
    fn get_current_time(&mut self) -> Result<std::time::Duration, CommonError>;
}

#[derive(Debug,Clone, Default)]
pub struct RelationalExecutorInfo {
    pub addr : String,
    pub name : String,
    pub user : String,
    pub password : String,
    pub timeout_sec : u32
}


pub type RelationalExecutorBox<T> = Box<dyn PoolItem<Box<dyn RelationalExecutor<T>>>>;
pub type RelationalExecutorPool<T> = Arc<dyn ThreadSafePool<Box<dyn RelationalExecutor<T>>,()>>;