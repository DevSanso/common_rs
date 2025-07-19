use super::types::*;

pub fn enum_option_get_only_one<E, T>(
    value: Option<E>,
) -> Result<T, String>
where
    E: IntoEnum<T>,
{
    match value {
        Some(e) => e.into_enum(),
        None => Err("enum:Unknown, sub:Unknown".to_string()), // 디폴트 메시지
    }
}

pub fn enum_get_only_one<E, T>(
    value: E,
) -> Result<T, String>
where
    E: IntoEnum<T>,
{
    value.into_enum()
}

pub fn enum_get_only_one_ref<E, T>(
    value: &E,
) -> Result<T, String>
where
    E: CloneEnum<T>,
{
    value.clone_enum()
}