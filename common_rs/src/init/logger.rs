use std::error::Error;
use std::fs::File;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::{Arc, Once, OnceLock};

use common_err::CommonError;
use common_err::gen::CommonDefaultErrorKind;
use common_logger::Logger;

pub type LoggerConf = common_logger::LoggerConfig;
pub(crate) static LOGGER_LAZY : OnceLock<Arc<dyn Logger>> = OnceLock::new();
static IS_INITIALIZED : Once = Once::new();
pub fn init_once(conf : LoggerConf) -> Result<(), CommonError> {
    let mut ret : Result<(), CommonError> = Ok(());

    IS_INITIALIZED.call_once(|| {
        let lr = common_logger::new_logger(conf);
        if lr.is_err() {
            ret = lr.err().unwrap().to_result();
            return;
        }
        LOGGER_LAZY.get_or_init(|| {
            lr.unwrap()
        });
    });
    
    ret
}