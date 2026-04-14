#[cfg(test)]
mod file_tests {
    use super::*;
    use std::{fs, thread};
    use std::path::PathBuf;
    use common_core::func;
    use common_logger::{LogLevel, Logger, LoggerConfig};

    fn test_dir(name: &str) -> String {
        let mut dir = std::env::temp_dir();
        dir.push(format!("rust_logger_test_{}", name));

        // 테스트 전 초기화
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        dir.to_str().unwrap().to_string()
    }

    fn setup_logger(name: &str) -> std::sync::Arc<dyn Logger> {
        let dir = test_dir(name);
        common_logger::new_logger(LoggerConfig::File(dir, LogLevel::Trace, 10000)).unwrap()
    }

    #[test]
    fn test_log_write() {
        let logger = setup_logger("write");

        logger.info("app",func!(), file!(), "hello world");

        let mut path = std::env::temp_dir();
        path.push("rust_logger_test_write/app.log");

        let content = fs::read_to_string(path).unwrap();

        assert!(content.contains("hello world"));
        assert!(content.contains("INFO"));
    }

    #[test]
    fn test_rotation() {
        let logger = setup_logger("rotation");

        for _ in 0..500 {
            logger.info("rotate",func!(), file!(), "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        }

        let mut log_path = std::env::temp_dir();
        log_path.push("rust_logger_test_rotation/rotate.log");

        let mut backup_path = std::env::temp_dir();
        backup_path.push("rust_logger_test_rotation/rotate_old.log");

        assert!(fs::metadata(log_path).is_ok());
        assert!(fs::metadata(backup_path).is_ok());
    }

    #[test]
    fn test_thread_safety() {
        let logger = setup_logger("thread");

        let handles: Vec<_> = (0..5)
            .map(|i| {
                let logger = logger.clone();
                thread::spawn(move || {
                    for j in 0..20 {
                        logger.info("multi",func!(), file!(), &format!("t{}-{}", i, j));
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        let mut path = std::env::temp_dir();
        path.push("rust_logger_test_thread/multi.log");

        let content = fs::read_to_string(path).unwrap();

        assert!(content.contains("t0-"));
        assert!(content.contains("t4-"));
    }
}

mod scylla_tests {
    use super::*;
    use std::{fs, thread};
    use std::collections::HashMap;
    use std::path::PathBuf;
    use common_core::func;
    use common_err::CommonError;
    use common_err::gen::CommonDefaultErrorKind;
    use common_logger::{LogLevel, Logger, LoggerConfig};
    use common_pair_exec::PairExecutorInfo;

    fn setup_logger(name: &str) -> std::sync::Arc<dyn Logger> {
        let read_toml : HashMap<String, String> = toml::from_str(include_str!("./tests.asset.toml")).map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::Etc, e.to_string())
        }).unwrap();

        common_logger::new_logger(LoggerConfig::Scylla(
            name.to_string(),
            read_toml["addr"].clone(),
            read_toml["name"].clone(),
            read_toml["user"].clone(),
            read_toml["password"].clone(), LogLevel::Trace, 360)).unwrap()
    }

    #[test]
    fn test_log_write() {
        let logger = setup_logger("write");
        logger.info("app",func!(), file!(), "hello world");
        logger.trace("name", "name", 3.0);
    }
    #[test]
    fn test_thread_safety() {
        let logger = setup_logger("thread");
        let cnt = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let handles: Vec<_> = (0..5)
            .map(|i| {
                let logger = logger.clone();
                let cnt_ref =cnt.clone();
                thread::spawn(move || {

                    for j in 0..20 {

                        logger.info("mutli",func!(), file!(),  &format!("t{}-{}", i, j));
                        cnt_ref.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        println!("{}", cnt.load(std::sync::atomic::Ordering::SeqCst));
    }
}