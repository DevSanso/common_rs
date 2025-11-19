mod utils;
pub mod gen;

use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::panic::Location;
use std::thread::ThreadId;

#[derive(Clone)]
pub struct ErrDataTuple(pub String, pub &'static str, pub i64, pub &'static dyn CommonErrorKind);

pub struct CommonError {
    cause : String,
    message : &'static str,
    thread_id : ThreadId,
    func : Vec<ErrDataTuple>,
}

pub trait CommonErrorKind {
    fn message(&self) -> &'static str;
    fn name(&self) -> &'static str;
}
impl CommonError {
    #[track_caller]
    pub fn new<S : AsRef<str>>(kind :&'static dyn CommonErrorKind, cause : S) -> CommonError {
        let func = utils::get_source_func_name(2);
        let loc = Location::caller();
        let (file, line) = (loc.file(), loc.line() as i64);

        CommonError {
            cause : cause.as_ref().to_string(),
            message : kind.message(),
            thread_id : std::thread::current().id(),
            func : vec![ErrDataTuple(func, file, line, kind)],
        }
    }

    #[track_caller]
    pub fn extend<S : AsRef<str>>(kind :&'static dyn CommonErrorKind, cause : S, prev : CommonError) -> CommonError {
        let func = utils::get_source_func_name(2);
        let loc = Location::caller();
        let (file, line) = (loc.file(), loc.line() as i64);

        let mut f = vec![ErrDataTuple(func, file, line, kind)];
        f.extend(prev.func.into_iter());
        CommonError {
            cause : cause.as_ref().to_string(),
            message : kind.message(),
            thread_id : std::thread::current().id(),
            func : f,
        }
    }

    pub fn get_cause (&self) -> String {self.cause.clone()}
    pub fn get_message(&self) -> &'static str {self.message}
    pub fn func_ref(&self) -> &'_ Vec<ErrDataTuple> {&self.func}
    pub fn func(&self) ->Vec<ErrDataTuple> {self.func.clone()}
    fn print_error(&self,f : &mut Formatter<'_>) -> std::fmt::Result {
        write!(f,"{:?} : cause={}\n \
        message={}\n", self.thread_id, self.cause, self.message)?;
        
        write!(f, "stack({})\n:", self.func.len())?;
        for pos in self.func.as_slice() {
            write!(f,"err= {}, file={}:{}, func={}\n", pos.3.name(), pos.1, pos.2, pos.0)?;
        }
        
        Ok(())
    }

    pub fn to_result<T, E>(self) -> Result<T, E> where Self: Into<E> {
        Err(self.into())
    }
}

impl Debug for CommonError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
       self.print_error(f)
    }
}

impl Display for CommonError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.print_error(f)
    }
}

impl Error for CommonError {}
