pub mod init;
pub mod utils;
pub mod db;

pub use common_core::err::{ErrorCode, ErrorCategory, ErrorDesc, COMMON_ERROR_CATEGORY};
pub use common_conn::COMMON_CONN_ERROR_CATEGORY;

pub mod logger {
    pub use log::debug;
    pub use log::error;
    pub use log::info;
    pub use log::trace;

    pub fn get_is_trace_level() -> bool {
        unsafe {
            crate::init::logger::LOGGER_FILE_LEVEL_IS_TRACE
        }
    }
}

pub mod signal {
    pub use crate::init::signal::SIGABRT;
    pub use crate::init::signal::SIGBUS;
    pub use crate::init::signal::SIGINT;
    pub use crate::init::signal::SIGPIPE;

    pub fn is_set_signal(num : i32) -> bool {
        crate::init::signal::is_set_signal(num)
    }
}

pub mod err {
    pub use common_core::err::{COMMON_ERROR_CATEGORY, create_error, ErrorCategory, ErrorCode};
}