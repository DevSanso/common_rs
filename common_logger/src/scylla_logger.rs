use std::path::PathBuf;
use std::sync::Mutex;
use common_core::core_err_log;
use common_err::CommonError;
use common_err::gen::CommonDefaultErrorKind;
use common_pair_exec::{PairExecutorInfo, PairExecutorPool, PairValueEnum};
use crate::LogLevel;

const LOG_TABLE : &'static str = "create table if not exists comm_log(
    identifier text, name text, log_timestamp timestamp, level text, message text,
    primary key (identifier, name, log_timestamp, level)
)";

const LOG_TRACE_TABLE : &'static str = "create table if not exists trace_log(
    identifier text, name text, log_timestamp timestamp, key text, value double
    primary key (identifier, name, log_timestamp, key)
)";

macro_rules! insert_query {
    ($ttl:expr) => {
        format!("insert into comm_log(identifier, name, log_timestamp, level, message) values (?, ?, , ?, ?) ttl {}", $ttl)
    };
    (trace, $ttl:expr) => {
        format!("insert into trace_log(identifier, name, log_timestamp, key, value) values (?, ?, , ?, ?) ttl {}", $ttl)
    };
}

pub(crate) struct ScyllaLogger {
    identifier : String,
    insert_query : String,
    insert_trace_query : String,
    pool : PairExecutorPool
}

impl ScyllaLogger {
    pub fn new(identifier : String, addr : String, dbname : String, user : String, passwd : String, ttl : u64) -> Result<Self, CommonError> {
        let p = common_exec_scylla::create_scylla_pair_conn_pool("logger".to_string(), PairExecutorInfo {
            addr : vec![addr],
            name: dbname,
            user,
            password: passwd,
            timeout_sec: 1,
            extend: None,
        }, 10);

        let mut get_ret = p.get_owned(()).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::ConnectFail, "", e)
        })?;

        let conn = get_ret.get_value();
        conn.execute_pair(LOG_TABLE, &PairValueEnum::Null).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::ExecuteFail, "", e)
        })?;
        conn.execute_pair(LOG_TRACE_TABLE, &PairValueEnum::Null).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::ExecuteFail, "", e)
        })?;

        get_ret.dispose();

        Ok(ScyllaLogger { identifier, pool: p, insert_query : insert_query!(ttl), insert_trace_query : insert_query!(trace, ttl) })
    }

    #[inline]
    fn create_log_param(&self, name : String, level : &'static str, message : String) -> PairValueEnum {
        PairValueEnum::Array(vec![
            PairValueEnum::String(self.identifier.to_string()),
            PairValueEnum::String(name),
            PairValueEnum::String(level.to_string()),
            PairValueEnum::String(message)
        ])
    }
}

impl crate::Logger for ScyllaLogger {
    fn debug(&self, name: &'_ str, message: &'_ str) {
        let conn_item_ret = self.pool.get_owned(()).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::ConnectFail, "", e)
        });

        if conn_item_ret.is_err() {
            core_err_log!("{}", conn_item_ret.err().unwrap().to_string());
            return;
        }

        let mut conn_item = conn_item_ret.unwrap();
        let conn = conn_item.get_value();

        let ret = conn.execute_pair(self.insert_query.as_str(),
                                    &self.create_log_param(name.to_string(), "DEBUG", message.to_string()));

        if ret.is_err() {
            core_err_log!("{}", ret.err().unwrap().to_string());
            conn_item.dispose();
        } else {
            conn_item.restoration();
        }
    }

    fn info(&self, name: &'_ str, message: &'_ str) {
        let conn_item_ret = self.pool.get_owned(()).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::ConnectFail, "", e)
        });

        if conn_item_ret.is_err() {
            core_err_log!("{}", conn_item_ret.err().unwrap().to_string());
            return;
        }

        let mut conn_item = conn_item_ret.unwrap();
        let conn = conn_item.get_value();

        let ret = conn.execute_pair(self.insert_query.as_str(),
                                    &self.create_log_param(name.to_string(), "INFO", message.to_string()));

        if ret.is_err() {
            core_err_log!("{}", ret.err().unwrap().to_string());
            conn_item.dispose();
        } else {
            conn_item.restoration();
        }
    }

    fn error(&self, name: &'_ str, message: &'_ str) {
        let conn_item_ret = self.pool.get_owned(()).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::ConnectFail, "", e)
        });

        if conn_item_ret.is_err() {
            core_err_log!("{}", conn_item_ret.err().unwrap().to_string());
            return;
        }

        let mut conn_item = conn_item_ret.unwrap();
        let conn = conn_item.get_value();

        let ret = conn.execute_pair(self.insert_query.as_str(),
                                    &self.create_log_param(name.to_string(), "ERROR", message.to_string()));

        if ret.is_err() {
            core_err_log!("{}", ret.err().unwrap().to_string());
            conn_item.dispose();
        } else {
            conn_item.restoration();
        }
    }

    fn trace(&self, name: &'_ str, key: &'_ str, value: f64) {
        let conn_item_ret = self.pool.get_owned(()).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::ConnectFail, "", e)
        });

        if conn_item_ret.is_err() {
            core_err_log!("{}", conn_item_ret.err().unwrap().to_string());
            return;
        }

        let mut conn_item = conn_item_ret.unwrap();
        let conn = conn_item.get_value();

        let ret = conn.execute_pair(self.insert_trace_query.as_str(), &PairValueEnum::Array(
            vec![
                PairValueEnum::String(self.identifier.to_string()),
                PairValueEnum::String(name.to_string()),
                PairValueEnum::String(key.to_string()),
                PairValueEnum::String(value.to_string())
            ]
        ));

        if ret.is_err() {
            core_err_log!("{}", ret.err().unwrap().to_string());
            conn_item.dispose();
        } else {
            conn_item.restoration();
        }
    }
}