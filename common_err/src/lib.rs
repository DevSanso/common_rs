mod utils;
mod gen;

use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::thread::ThreadId;

pub struct CommonError {
    cause : String,
    message : &'static str,
    func_name : String,
    line : i64,
    file : &'static str,
    thread_id : ThreadId
}

pub trait CommonErrorKind {
    fn message(&self) -> &'static str;
}

impl CommonError {
    #[track_caller]
    pub fn new(kind :&'_ dyn CommonErrorKind, cause : String) -> CommonError {
        let func = utils::get_source_func_name(1);
        let (file, line) = utils::get_source_file_and_line();

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
