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
    use log;

    pub use log::debug as log_debug;
    pub use log::info as log_info;
    pub use log::warn as log_warn;
    pub use log::error as log_error;
}