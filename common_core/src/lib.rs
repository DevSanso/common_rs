pub mod collection;
pub mod utils;


pub mod logger {
    pub use log;
    #[macro_export]
    macro_rules! log_debug {
        ($($arg:tt)+) => {
            {
                let thread_id = std::thread::current().id();
                $crate::logger::log::debug!("{:?} at {:.1024}", thread_id, format!($($arg)*));
            }
        };
    }

    #[macro_export]
    macro_rules! log_warn {
        ($($arg:tt)+) => {
            {
                let thread_id = std::thread::current().id();
                $crate::logger::log::warn!("{:?} at {:.1024}", thread_id, format!($($arg)*));
            }
        };
    }

    #[macro_export]
    macro_rules! log_trace {
        ($($arg:tt)+) => {
            {
                let thread_id = std::thread::current().id();
                $crate::logger::log::trace!("{:?} at {}", thread_id, format!($($arg)*));
            }
        };
    }

    #[macro_export]
    macro_rules! log_error {
        ($($arg:tt)+) => {
            {
                let thread_id = std::thread::current().id();
                $crate::logger::log::error!("{:?} at \n{}", thread_id, format!($($arg)*));
            }
        };
    }

    #[macro_export]
    macro_rules! log_info {
        ($($arg:tt)+) => {
            {
                let thread_id = std::thread::current().id();
                $crate::logger::log::info!("{:?} at {:.1024}", thread_id, format!($($arg)*));
            }
        };
    }

    pub use log_debug;
    pub use log_warn;
    pub use log_info;
    pub use log_trace;
    pub use log_error;
}