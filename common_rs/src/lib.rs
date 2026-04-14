pub mod init;

pub use common_core as c_core;
pub use common_err as c_err;
pub use common_thread as th;
pub mod exec {
    pub mod interfaces {
        pub use common_pair_exec as pair;
    }

    pub use common_exec_duckdb as duckdb;
    pub use common_exec_scylla as scylla;
    pub use common_exec_pg as pg;
    pub use common_exec_redis as redis;
    pub use common_exec_odbc as odbc;
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

pub mod logger {
    #[macro_export]
    macro_rules! log_debug {
        ($name:expr, $($arg:tt)+) => {
            {
                let caller = std::panic::Location::caller();
                let file = caller.file();

                let l = $crate::init::logger::LOGGER_LAZY.get().expect("single logger is none");
                l.debug($name, $crate::c_core::utils::macros::func!(), file, format!($($arg)*));
            }
        };
    }

    #[macro_export]
    macro_rules! log_info {
        ($name:expr, $($arg:tt)+) => {
            {
                let caller = std::panic::Location::caller();
                let file = caller.file();

                let l = $crate::init::logger::LOGGER_LAZY.get().expect("single logger is none");
                l.info($name, $crate::c_core::utils::macros::func!(), file, format!($($arg)*));
            }
        };
    }

    #[macro_export]
    macro_rules! log_error {
        ($name:expr, $($arg:tt)+) => {
            {
                let caller = std::panic::Location::caller();
                let file = caller.file();

                let l = $crate::init::logger::LOGGER_LAZY.get().expect("single logger is none");
                l.error($name, $crate::c_core::utils::macros::func!(), file, format!($($arg)*));
            }
        };
    }

    #[macro_export]
    macro_rules! log_trace {
        ($name:expr, $key:expr, $value:expr) => {
            {
                let l = $crate::init::logger::LOGGER_LAZY.get().expect("single logger is none");
                l.trace($name, $key, $value);
            }
        };
    }

    pub use log_debug;
    pub use log_info;
    pub use log_error;
    pub use log_trace;
}