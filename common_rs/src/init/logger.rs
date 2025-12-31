use std::error::Error;
use std::fs::File;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::{Once, OnceLock};

use logforth::append;
use logforth::record::Level;
use logforth::record::LevelFilter;
use common_err::CommonError;
use common_err::gen::CommonDefaultErrorKind;
use logforth;
use logforth::append::file::FileBuilder;
use logforth::starter_log::LogStarterBuilder;

fn convert_str_to_log_level(log_level : &'_ str) -> Level {
    match log_level {
        "debug" => Level::Debug,
        "warn" => Level::Warn,
        "trace" => Level::Trace,
        "info" => Level::Info,
        _ => Level::Error
    }
}

static LOGGER_INIT_ONCE : Once = Once::new();

fn set_log_builder(level: Level, base_dir_opt : Option<&'_ str>, max_size : usize, builder : LogStarterBuilder) -> Result<LogStarterBuilder, CommonError> {
    const LOG_LEVEL_FILE_NAME : &[&'static str] = &[
        "error.log",
        "warn.log",
        "info.log",
        "debug.log",
        "trace.log",
    ];

    const LOG_LEVEL_THRESHOLD : &[Level] = &[
        Level::Error,
        Level::Warn,
        Level::Info,
        Level::Debug,
        Level::Trace
    ];

    let mut dispatch = builder;
    let max_idx = LOG_LEVEL_THRESHOLD.iter().position(|l| *l == level).map_or(0, |i| i);

    for dispatch_idx in 0..max_idx {
        if let Some(base_dir) = base_dir_opt.as_ref() {
            let file = FileBuilder::new(base_dir, LOG_LEVEL_FILE_NAME[dispatch_idx])
                .rollover_size(NonZeroUsize::new(max_size).expect("NonZeroUsize New is Broken"))
                .max_log_files(NonZeroUsize::new(5).expect("NonZeroUsize New is Broken"))
                .build()
                .map_err(|e| {
                    CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, e.to_string())
                })?;

            dispatch = dispatch.dispatch(|d| {
                d.filter(LevelFilter::MoreSevereEqual(LOG_LEVEL_THRESHOLD[dispatch_idx]))
                    .append(file)
            });
        } else {
            let stdout = append::Stdout::default();
            dispatch = dispatch.dispatch(|d| {
                d.filter(LevelFilter::MoreSevereEqual(LOG_LEVEL_THRESHOLD[dispatch_idx]))
                    .append(stdout)
            });
        }
    }
    Ok(dispatch)
}
pub(crate) static LOGGER_FILE_LEVEL_IS_TRACE : OnceLock<bool> = OnceLock::new();

pub fn init_once(log_level : &'_ str, log_dir : Option<&'_ str>, max_size : usize) -> Result<(), CommonError> {
    let mut ret : Result<(), CommonError> = Ok(());

    if let Some(log_dir_path) = log_dir {
        if !std::fs::exists(log_dir_path).map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::SystemCallFail, e.to_string())
        })? {
            return CommonError::new(&CommonDefaultErrorKind::SystemCallFail, format!("not exists dir {}", log_dir_path)).to_result();
        }
    }

    LOGGER_INIT_ONCE.call_once(|| {
        let level = convert_str_to_log_level(log_level);
        if level == Level::Trace {
            LOGGER_FILE_LEVEL_IS_TRACE.get_or_init(|| true);
        }

        let builder = set_log_builder(level, log_dir, max_size,logforth::starter_log::builder()).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::InitFailed, "log builder failed", e).to_result()
        });

        if let Err(e) = builder {
            ret = e
        } else {
            builder.expect("builder expect is broken").apply()
        }
    });

    ret
}