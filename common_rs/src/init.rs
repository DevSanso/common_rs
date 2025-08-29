pub mod signal;
pub mod logger;
pub mod err_code;

use std::error::Error;

use common_conn::err::COMMON_CONN_ERROR_CATEGORY;
use common_core::err::API_CALL_ERROR;

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
pub fn init_common(logger : LoggerConfig<'_>, errors : Option<ErrorCodeConfig>) -> Result<(), Box<dyn Error>>{
    signal::init_once();

    logger::init_once(logger.log_level, logger.log_file)?;

    if errors.is_some() {
        let e = errors.unwrap();
        if e.category_id == COMMON_ERROR_CATEGORY || e.category_id == COMMON_CONN_ERROR_CATEGORY {
            return crate::err::create_error(COMMON_ERROR_CATEGORY,
                 API_CALL_ERROR, format!("already exists {}", e.category_id), None).as_error();
            //return crate::error_code::common_make_err!(COMMON_ERROR_CATEGORY, ApiCallError)
        }
    }

    common_conn::err::common_conn_init();
    
    Ok(())
}