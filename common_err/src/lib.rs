mod utils;
pub mod gen;

use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::panic::Location;
use std::thread::ThreadId;

pub struct CommonError {
    cause : String,
    message : &'static str,
    func_name : String,
    line : i64,
    file : &'static str,
    thread_id : ThreadId
}

pub struct CommonErrors {
    title : &'static str,
    errs : Vec<CommonError>,
    thread_id : ThreadId,
}

pub trait CommonErrorKind {
    fn message(&self) -> &'static str;
}

impl CommonErrors {
    pub fn new(title : &'static str) -> Self {
        CommonErrors {title, errs : Vec::with_capacity(3), thread_id : std::thread::current().id()}
    }

    pub fn len(&self) -> usize {self.errs.len()}

    pub fn push(&mut self, error : CommonError) {
        self.errs.push(error);
    }

    fn print_error(&self,f : &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "errorList({:?}): {}\n", self.thread_id, self.title)?;
        let mut buf = String::with_capacity(1024);
        for e in &self.errs {
            let line = format!("\tcause={}, file={}:{}, func={}, message={}\n"
                               , e.message, e.file, e.line, e.func_name, e.message);
            buf.push_str(line.as_str());
        }
        write!(f,"{}", buf.as_str())
    }

    pub fn to_result<T, E>(self) -> Result<T, E> where Self: Into<E> {
        Err(self.into())
    }
}
impl CommonError {
    #[track_caller]
    pub fn new(kind :&'_ dyn CommonErrorKind, cause : String) -> CommonError {
        let func = utils::get_source_func_name(2);
        let loc = Location::caller();
        let (file, line) = (loc.file(), loc.line() as i64);

        CommonError {
            cause,
            message : kind.message(),
            file,
            line,
            func_name : func,
            thread_id : std::thread::current().id()
        }
    }

    pub fn get_cause (&self) -> String {self.cause.clone()}
    pub fn get_message(&self) -> &'static str {self.message}
    pub fn get_file(&self) -> &'static str {self.file}
    pub fn get_line(&self) -> i64 {self.line}
    pub fn get_func(&self) -> String { self.func_name.clone() }
    
    fn print_error(&self,f : &mut Formatter<'_>) -> std::fmt::Result {
        write!(f,"{:?} : cause {}\n \
        message={}\n \
        file={}:{}\n \
        func={}", self.thread_id, self.cause, self.message, self.file, self.line, self.func_name)
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
impl Debug for CommonErrors {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.print_error(f)
    }
}

impl Display for CommonErrors {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.print_error(f)
    }
}

impl Error for CommonErrors {}

impl Error for CommonError {}
