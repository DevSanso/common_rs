use std::error::Error;
use std::sync::Once;
use std::process::Command;

use ftail::Ftail;
use log::LevelFilter;

use common_core::err::*;

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

pub fn init_once(log_level : &'_ str, log_file : Option<&'_ str>) -> Result<(), Box<dyn Error>> {
    let mut ret : Result<(), Box<dyn Error>> = Ok(());

    LOGGER_INIT_ONCE.call_once(|| {
        let level = convert_str_to_log_level(log_level);
        let mut ftail = Ftail::new().datetime_format("%Y-%m-%d %H:%M:%S%.3f");
        
        if level == LevelFilter::Trace {
            ftail = ftail.console(LevelFilter::Trace);
        } else {
            ftail = ftail.console(LevelFilter::Debug);
        }

        if log_file.is_some() {
            let file = log_file.unwrap();

            let output_opt = Command::new("sh")
                .arg("-c")
                .arg(format!("echo {}", file))
                .output()
                .ok();
            
            if output_opt.is_none() {
                ret = create_error(COMMON_ERROR_CATEGORY, 
                    API_CALL_ERROR, 
                    "sh failed".to_string(), None).as_error();
                return;
            }

            let output = output_opt.unwrap();
            if !output.status.success() {
                ret = create_error(COMMON_ERROR_CATEGORY, 
                    API_CALL_ERROR, 
                    "sh failed".to_string(), None).as_error();
                return;
            }

            {
                let file_path = String::from_utf8_lossy(&output.stdout);
                let chk_write = std::fs::OpenOptions::new().write(true).open(file_path.trim().to_string());
            
                if chk_write.is_err() {
                    ret = create_error(COMMON_ERROR_CATEGORY, 
                        API_CALL_ERROR, 
                        "".to_string(), Some(Box::new(chk_write.unwrap_err()))).as_error();
                    return;
                }
            }

            ftail = ftail.single_file(file, true, level);
        }
        
        unsafe {
            if level == LevelFilter::Trace {
                LOGGER_FILE_LEVEL_IS_TRACE = true;
            }

            ret = match ftail.init() {
                Ok(_) => Ok(()),
                Err(e) => create_error(COMMON_ERROR_CATEGORY, 
                    API_CALL_ERROR, 
                    "".to_string(), Some(Box::new(e))).as_error()
            }
        }
    });

    ret
}