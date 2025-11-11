use std::fmt::{Debug, Display, Formatter};

pub trait IntoEnum<T> {
    fn into_enum(self) -> Result<T, String>;
}

pub trait CloneEnum<T> {
    fn clone_enum(&self) -> Result<T, String>;
}

pub struct SimpleError {
    pub msg: String
}

impl SimpleError {
    pub fn to_result<T, E>(self) -> Result<T, E>
    where
        Self: Into<E>
    {
        Err(self.into())
    }
}

impl Debug for SimpleError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.msg)
    }
}

impl Display for SimpleError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.msg)
    }
}

impl std::error::Error for SimpleError {}