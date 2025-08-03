pub mod signal;
pub mod logger;
pub mod err_code;

use std::error::Error;

use crate::ErrorCategory;
use crate::ErrorCode;
use crate::ErrorDesc;
use crate::COMMON_ERROR_CATEGORY;

pub struct LoggerConfig<'a> {
    pub log_level : &'a str,
    pub log_file : Option<&'a str>
}

pub struct ErrorCodeConfig {
    pub category_id :ErrorCategory, 
    pub errs : Vec<(ErrorCode, ErrorDesc)>
}
pub fn init_common(logger : LoggerConfig<'_>, errors : ErrorCodeConfig) -> Result<(), Box<dyn Error>>{
    signal::init_once();

    logger::init_once(logger.log_level, logger.log_file)?;

    if errors.category_id == COMMON_ERROR_CATEGORY {
        //return crate::error_code::common_make_err!(COMMON_ERROR_CATEGORY, ApiCallError)
    }

    common_conn::err::common_conn_init();
    
    Ok(())
}