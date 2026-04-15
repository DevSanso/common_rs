pub(crate) mod signal;
pub mod logger;

use common_err::CommonError;
use common_err::gen::CommonDefaultErrorKind;

pub use crate::init::logger::LoggerConf;

pub struct InitConfig {
    pub logger_conf : LoggerConf,
}
pub fn convert_str_to_log_level(log_level : &'_ str) -> common_logger::LogLevel {
    match log_level {
        "debug" => common_logger::LogLevel::Debug,
        "trace" => common_logger::LogLevel::Trace,
        "info" => common_logger::LogLevel::Info,
        _ => common_logger::LogLevel::Error
    }
}
pub fn init_common(cfg : InitConfig) -> Result<(), CommonError>{
    signal::init_once();
    logger::init_once(cfg.logger_conf).map_err(|e| {
        CommonError::extend(&CommonDefaultErrorKind::InitFailed, "can't success init_once", e)
    })?;
    Ok(())
}