use std::{fs, fs::{OpenOptions, rename, metadata}, io::Write, path::PathBuf, sync::Mutex, thread};

use chrono::Local;
use common_err::CommonError;
use common_err::gen::CommonDefaultErrorKind;
use crate::LogLevel;

pub(crate) struct ConsoleLogger {
    lock: Mutex<()>,
}

impl ConsoleLogger {
    pub fn new() -> Result<Self, CommonError> {
        Ok(ConsoleLogger {lock: Mutex::new(())})
    }

    fn now_string() -> String {
        Local::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string()
    }

    fn thread_id() -> String {
        format!("{:?}", thread::current().id())
    }

    fn format_line(&self, name: &str, file: &str, func: &str, level: &str, message: &str) -> String {
        format!(
            "{:21},{:10}({:128}:{:256}) at {:.60}({:.10}):{}",
            Self::now_string(),
            Self::thread_id(),
            file,
            func,
            name,
            level,
            message
        )
    }

    fn format_trace_line(&self, name: &str, level: &str, message: &str) -> String {
        format!(
            "{:21},{:10} at {:.60}({:.10}):{}",
            Self::now_string(),
            Self::thread_id(),
            name,
            level,
            message
        )
    }

    fn write_line(&self, name: &str, line: &str) {
        let _guard = self.lock.lock().unwrap();
        let _ = println!("{}", line);
    }
}

impl crate::Logger for ConsoleLogger {
    fn debug(&self, name: &str, func : &'_ str, file : &'_ str, message: &str) {
        let line = self.format_line(name, file, func, "DEBUG", message);
        self.write_line(name, &line);
    }

    fn info(&self, name: &str, func : &'_ str, file : &'_ str, message: &str) {
        let line = self.format_line(name, file, func, "INFO", message);
        self.write_line(name, &line);
    }

    fn error(&self, name: &str, func : &'_ str, file : &'_ str, message: &str) {
        let line = self.format_line(name, file, func, "ERROR", message);
        self.write_line(name, &line);
    }

    fn trace(&self, name: &str, key: &str, value: f64) {
        let msg = format!("{}={}", key, value);
        let line = self.format_trace_line(name, "TRACE", &msg);
        self.write_line(name, &line);
    }
}