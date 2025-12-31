pub mod collection;
pub mod utils;


pub mod logger {
    pub use log;
    #[macro_export]
    macro_rules! log_debug {
        ($message:expr) => {
            {
                let caller = std::panic::Location::caller();
                let func = $crate::utils::macros::func!();
                let line = caller.line();
                let file = caller.file();
                let thread_id = std::thread::current().id();
                
                $crate::logger::log::debug!("{:?} at {}:{}:{}\n{:.1024}", thread_id, file, line, line, $message);
            }
        };
    }

    #[macro_export]
    macro_rules! log_warn {
        ($message:expr) => {
            {
                let caller = std::panic::Location::caller();
                let func = $crate::utils::macros::func!();
                let line = caller.line();
                let file = caller.file();
                let thread_id = std::thread::current().id();
                
                $crate::logger::log::warn!("{:?} at {}:{}:{}\n{:.1024}", thread_id, file, line, line, $message);
            }
        };
    }

    #[macro_export]
    macro_rules! log_trace {
        ($message:expr) => {
            {
                let caller = std::panic::Location::caller();
                let func = $crate::utils::macros::func!();
                let line = caller.line();
                let file = caller.file();
                let thread_id = std::thread::current().id();
                
                $crate::logger::log::trace!("{:?} at {}:{}:{}\n{}", thread_id, file, line, line, $message);
            }
        };
    }

    #[macro_export]
    macro_rules! log_error {
        ($message:expr) => {
            {
                let caller = std::panic::Location::caller();
                let func = $crate::utils::macros::func!();
                let line = caller.line();
                let file = caller.file();
                let thread_id = std::thread::current().id();
                
                $crate::logger::log::error!("{:?} at {}:{}:{}\n{}", thread_id, file, line, line, $message);
            }
        };
    }

    #[macro_export]
    macro_rules! log_info {
        ($message:expr) => {
            {
                let caller = std::panic::Location::caller();
                let func = $crate::utils::macros::func!();
                let line = caller.line();
                let file = caller.file();
                let thread_id = std::thread::current().id();
                
                $crate::logger::log::info!("{:?} at {}:{}:{}\n{:.1024}", thread_id, file, line, line, $message);
            }
        };
    }
    
    pub use log_debug;
    pub use log_warn;
    pub use log_info;
    pub use log_trace;
    pub use log_error;
}