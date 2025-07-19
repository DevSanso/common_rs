pub trait IntoEnum<T> {
    fn into_enum(self) -> Result<T, String>;
}

pub trait CloneEnum<T> {
    fn clone_enum(&self) -> Result<T, String>;
}