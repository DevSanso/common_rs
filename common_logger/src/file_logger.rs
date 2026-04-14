use std::{fs, fs::{OpenOptions, rename, metadata}, io::Write, path::PathBuf, sync::Mutex, thread};

use chrono::Local;
use common_err::CommonError;
use common_err::gen::CommonDefaultErrorKind;
use crate::LogLevel;

pub(crate) struct FileLogger {
    dir: PathBuf,
    max_size: u64,
    level : LogLevel,
    lock: Mutex<()>,
}

impl FileLogger {
    pub fn new(dir: &'_ dyn AsRef<str>, level : LogLevel, max_size: u64) -> Result<Self, CommonError> {
        let dir_path = PathBuf::from(dir.as_ref());

        let is_ext = fs::exists(dir_path.as_path()).map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::SystemCallFail, e.to_string())
        })?;

        if !is_ext {
            return CommonError::new(&CommonDefaultErrorKind::NoData, format!("not exists : {:?}", dir_path)).to_result();
        }

        Ok(Self {
            dir: dir_path,
            max_size,
            level,
            lock: Mutex::new(()),
        })
    }

    fn log_path(&self, name: &str) -> PathBuf {
        self.dir.join(format!("{}.log", name))
    }

    fn backup_path(&self, name: &str) -> PathBuf {
        self.dir.join(format!("{}_old.log", name))
    }

    fn rotate_if_needed(&self, name: &str) {
        let path = self.log_path(name);

        if let Ok(meta) = metadata(&path) {
            if meta.len() >= self.max_size {
                let backup = self.backup_path(name);

                let _ = std::fs::remove_file(&backup);
                let _ = rename(&path, &backup);
            }
        }
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

        self.rotate_if_needed(name);

        let path = self.log_path(name);

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .unwrap();

        let _ = writeln!(file, "{}", line);
    }
}

impl crate::Logger for FileLogger {
    fn debug(&self, name: &str, func : &'_ str, file : &'_ str, message: &str) {
        if self.level < LogLevel::Debug {
            return;
        }
        let line = self.format_line(name, file, func, "DEBUG", message);
        self.write_line(name, &line);
    }

    fn info(&self, name: &str, func : &'_ str, file : &'_ str, message: &str) {
        if self.level < LogLevel::Info {
            return;
        }
        let line = self.format_line(name, file, func, "INFO", message);
        self.write_line(name, &line);
    }

    fn error(&self, name: &str, func : &'_ str, file : &'_ str, message: &str) {
        let line = self.format_line(name, file, func, "ERROR", message);
        self.write_line(name, &line);
    }

    fn trace(&self, name: &str, key: &str, value: f64) {
        if self.level < LogLevel::Trace {
            return;
        }
        let msg = format!("{}={}", key, value);
        let line = self.format_trace_line(name, "TRACE", &msg);
        self.write_line(name, &line);
    }
}