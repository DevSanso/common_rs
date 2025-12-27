use std::error::Error;
use std::path::Path;
use std::sync::Once;

use ftail::Ftail;
use log::LevelFilter;
use common_err::CommonError;
use common_err::gen::CommonDefaultErrorKind;

fn convert_str_to_log_level(log_level : &'_ str) -> LevelFilter {
    match log_level {
        "debug" => LevelFilter::Debug,
        "warn" => LevelFilter::Warn,
        "trace" => LevelFilter::Trace,
        "info" => LevelFilter::Info,
        _ => LevelFilter::Error
    }
}

static LOGGER_INIT_ONCE : Once = Once::new();
pub(crate) static mut LOGGER_FILE_LEVEL_IS_TRACE : bool = false;

pub fn init_once(log_level : &'_ str, log_file : Option<&'_ str>, max_size : u64) -> Result<(), CommonError> {
    let mut ret : Result<(), CommonError> = Ok(());

    LOGGER_INIT_ONCE.call_once(|| {
        let level = convert_str_to_log_level(log_level);
        let mut ftail = Ftail::new().datetime_format("%Y-%m-%d %H:%M:%S%.3f").max_file_size(max_size);

        if level == LevelFilter::Trace {
            ftail = ftail.console(LevelFilter::Trace);
        } else {
            ftail = ftail.console(LevelFilter::Debug);
        }

        if log_file.is_some() {
            let file_path = log_file.unwrap();
            {
                let chk_write = std::fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .append(true)
                    .open(file_path.trim().to_string());

                if chk_write.is_err() {
                    ret = CommonError::new(&CommonDefaultErrorKind::SystemCallFail, format!("common_rs - logger,init,chk - {}:{}", file_path, chk_write.err().unwrap()))
                        .to_result();
                    return;
                }
            }
            ftail = ftail.single_file(Path::new(file_path), true, level);
        }

        unsafe {
            if level == LevelFilter::Trace {
                LOGGER_FILE_LEVEL_IS_TRACE = true;
            }

            ret = match ftail.init() {
                Ok(_) => Ok(()),
                Err(e) => CommonError::new(&CommonDefaultErrorKind::InvalidApiCall, format!("common_rs - logger,init,console\
                 - {}", e)).to_result()
            }
        }
    });

    ret
}