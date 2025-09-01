pub mod err;

use std::error::Error;
use std::sync::Arc;

use common_core::collection::pool::ThreadSafePool;
use common_core::collection::pool::PoolItem;

#[derive(Clone, Debug, PartialEq)]
pub enum CommonValue {
    Double(f64),
    Int(i32),
    BigInt(i64),
    String(String),
    Binrary(Vec<u8>),
    Bool(bool),
    Float(f32),
    Null
}

#[derive(Default,Clone)]
pub struct CommonSqlExecuteResultSet {
    pub cols_name : Vec<String>,
    pub cols_data : Vec<Vec<CommonValue>>
}
pub trait CommonSqlConnection {
    fn execute(&mut self, query : &'_ str, param : &'_ [CommonValue]) -> Result<CommonSqlExecuteResultSet, Box<dyn Error>>;
    fn get_current_time(&mut self) -> Result<std::time::Duration, Box<dyn Error>>;
    fn trans(&mut self) -> Result<&mut dyn CommonSqlTxConnection, Box<dyn Error>>;
}

pub trait CommonSqlTxConnection {
    fn execute_tx(&mut self, query : &'_ str, params : &'_[&'_ [CommonValue]]) -> Result<(), Box<dyn Error>>;
}

#[derive(Debug,Clone)]
pub struct CommonSqlConnectionInfo {
    pub addr : String,
    pub db_name : String,
    pub user : String,
    pub password : String,
    pub timeout_sec : u32
}

pub type CommonSqlConnectionBox = Box<dyn PoolItem<Box<dyn CommonSqlConnection>>>;
pub type CommonSqlConnectionPool = Arc<dyn ThreadSafePool<Box<dyn CommonSqlConnection>,()>>;

