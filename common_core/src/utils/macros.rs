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

pub use func;

#[macro_export]
macro_rules! gen_err_msg_list {
    (
        $(($ident:ident, $desc:expr, $detail:expr)),* $(,)?
    ) => {
        $(
            pub const $ident: &str = stringify!($ident);
        )*

        pub(crate) fn _gen_err_list() -> Vec<(&'static str, (&'static str, &'static str))> {
            vec![
                $(
                    (stringify!($ident), ($desc, $detail)),
                )*
            ]
        }
    };
}

pub use gen_err_msg_list;

