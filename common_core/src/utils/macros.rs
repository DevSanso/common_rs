#[macro_export]
macro_rules! enum_option_get_only_one {
    ($enum_name:ident, $sub_enum_name:ident, $body:expr) => {
        match $body {
            Some($enum_name::$sub_enum_name(c)) => Ok(c),
            _ => common_make_err!(data, ParsingError, "enum:{}, sub:{}", stringify!($enum_name), stringify!($sub_enum_name)),
        }
    };
}

#[macro_export]
macro_rules! enum_get_only_one {
    ($enum_name:ident, $sub_enum_name:ident, $body:expr) => {
        match $body {
            $enum_name::$sub_enum_name(c) => Ok(c),
            _ => common_make_err!(data, ParsingError, "enum:{}, sub:{}", stringify!($enum_name), stringify!($sub_enum_name)),
        }
    };
}

#[macro_export]
macro_rules! enum_get_only_one_ref {
    ($enum_name:ident, $sub_enum_name:ident, $body:expr) => {
        match &$body {
            $enum_name::$sub_enum_name(c) => Ok(c.clone()),
            _ => common_make_err!(data, ParsingError, "enum:{}, sub:{}", stringify!($enum_name), stringify!($sub_enum_name)),
        }
    };
}

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
macro_rules! common_make_err {
    ($category:expr ,$code : ident, $($arg:tt)+) => {{
        use common_core::utils::macros::func;
        use common_core::err::create_error;

        Err(Box::new(create_error(func!(), file!(), $category, stringify!($code), format!($($arg)+))))
    }};

    ($category:expr ,$code : ident) => {{
        use common_core::utils::macros::func;
        use common_core::err::create_error;

        Err(Box::new(create_error(func!(), file!(), $category, stringify!($code), "".to_string())))
    }};
}

macro_rules! common_make_err_crate {
    ($category:expr ,$code : ident, $($arg:tt)+) => {{
        use crate::utils::macros::func;
        use crate::err::create_error;

        Err(Box::new(create_error(func!(), file!(), $category, stringify!($code), format!($($arg)+))))
    }};

    ($category:expr ,$code : ident) => {{
        use crate::utils::macros::func;
        use crate::err::create_error;

        Err(Box::new(create_error(func!(), file!(), $category, stringify!($code), "".to_string())))
    }};
}
pub(crate) use common_make_err_crate;

