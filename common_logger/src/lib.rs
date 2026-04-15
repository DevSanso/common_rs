use std::sync::Arc;
use common_err::CommonError;
use crate::console_logger::ConsoleLogger;
use crate::file_logger::FileLogger;
use crate::scylla_logger::ScyllaLogger;

mod file_logger;
mod scylla_logger;
mod console_logger;

#[derive(PartialOrd, PartialEq)]
pub enum LogLevel {
    Error = 1,
    Info = 2,
    Debug = 3,
    Trace = 4,
}

pub trait Logger : Send + Sync {
    fn debug(&self, name : &'_ str, func : &'_ str, file : &'_ str, message : &'_ str);
    fn info(&self, name : &'_ str, func : &'_ str, file : &'_ str, message: &'_ str);
    fn error(&self, name : &'_ str, func : &'_ str, file : &'_ str, message: &'_ str);
    fn trace(&self, name : &'_ str, key : &'_ str, value : f64);
}

pub enum LoggerConfig {
    File(String, LogLevel, u64),
    Scylla(String, String, String, String, String, LogLevel, u64),
    Console
}

pub fn new_logger(config: LoggerConfig) -> Result<Arc<dyn Logger>, CommonError> {
    match config {
        LoggerConfig::File(dir,level, size) => {
            Ok(Arc::new(FileLogger::new(&dir, level, size)?))
        },
        LoggerConfig::Scylla(name, addr, dbname, user, passwd, level, ttl) => {
            Ok(Arc::new(ScyllaLogger::new(name, addr, dbname, user, passwd, level, ttl)?))
        },
        LoggerConfig::Console => {
            Ok(Arc::new(ConsoleLogger::new()?))
        }
    }
}