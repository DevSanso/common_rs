#[macro_export]
macro_rules! func {
    () => {
        {
            fn f() {}
            fn type_name_of<T>(_: T) -> &'static str {
                std::any::type_name::<T>()
            }
            let name = type_name_of(f);
            &name[..name.len() - 3]
        }
    };
}

#[macro_export]
macro_rules! core_err_log {
    ($($arg:tt)*) => {{
        use chrono::Local;

        let now = Local::now().format("%Y-%m-%d %H:%M:%S%.3f");
        eprintln!("{} - {} - {}", now, $crate::utils::macros::func!(), format!($($arg)*));
    }};
}
pub use func;
pub use core_err_log;

