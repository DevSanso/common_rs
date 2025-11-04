use std::error::Error;
use std::sync::Arc;

use common_core::collection::pool::ThreadSafePool;
use common_core::collection::pool::PoolItem;

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


#[derive(Default,Clone)]
pub struct RelationalExecuteResultSet {
    pub cols_name : Vec<String>,
    pub cols_data : Vec<Vec<RelationalValue>>
}

pub trait RelationalExecutor<PARAM> {
    fn execute(&mut self, query : &'_ str, param : &[PARAM]) -> Result<RelationalExecuteResultSet, Box<dyn Error>>;
    fn get_current_time(&mut self) -> Result<std::time::Duration, Box<dyn Error>>;
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