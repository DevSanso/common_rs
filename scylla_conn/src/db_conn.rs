mod utils;

use std::error::Error;

use scylla::Session;
use scylla::serialize::value::SerializeValue;
use tokio::runtime::{Builder, Runtime};

use common_core::err::{create_error};
use common_conn::{CommonSqlConnection, CommonValue, CommonSqlExecuteResultSet, CommonSqlConnectionInfo};
use common_conn::err::*;
use scylla::SessionBuilder;
use crate::db_conn::utils::ScyllaFetcher;
pub struct ScyllaCommonSqlConnection {
    session : Session,
    rt : Runtime
}

impl ScyllaCommonSqlConnection {
    pub(crate) fn new(infos : Vec<CommonSqlConnectionInfo>) -> Result<Self, Box<dyn Error>> {
        if infos.len() <= 0 {
            return create_error(COMMON_CONN_ERROR_CATEGORY, 
                GET_CONNECTION_FAILED_ERROR, 
                "scylla connection info array size of zero".to_string()).as_error();
        }
     
        let mut builder = SessionBuilder::new();
        
        for info in infos {
            builder = builder
                .known_node(info.addr)
                .user(info.user, info.password)
                .use_keyspace(info.db_name, false);
        }

        let rt = Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

        let feature = builder.build();
        let block = rt.block_on(feature);

        match block {
            Ok(ok) => Ok(ScyllaCommonSqlConnection{session : ok, rt : rt}),
            Err(err) => create_error(COMMON_CONN_ERROR_CATEGORY, 
                GET_CONNECTION_FAILED_ERROR, 
                err.to_string()).as_error()
        }
    }
}
impl CommonSqlConnection for ScyllaCommonSqlConnection {
    fn execute(&mut self, query : &'_ str, param : &'_ [CommonValue]) -> Result<CommonSqlExecuteResultSet, Box<dyn Error>> {
        common_core::logger::trace!("ScyllaCommonSqlConnection - prepare query:{} param:{:?}", query, param);

        let feature = self.session.prepare(query);

        let prepare = match self.rt.block_on(feature) {
            Ok(ok) => Ok(ok),
            Err(err) => create_error(COMMON_CONN_ERROR_CATEGORY, 
                CONNECTION_API_CALL_ERROR, 
                err.to_string()).as_error()
        }?;

        let mut result = CommonSqlExecuteResultSet::default();

        let mut typ = Vec::new();
        for col in prepare.get_result_set_col_specs() {
            result.cols_name.push(col.name().to_string());
            typ.push(col.typ());
        }

        let real_param = param.iter().fold(Vec::<Option<&dyn SerializeValue>>::new(), |mut acc,x | {
            let p : Option<&dyn SerializeValue> = match x {
                CommonValue::Int(i) => Some(i),
                CommonValue::Binrary(bs) => Some(bs),
                CommonValue::Double(f) => Some(f),
                CommonValue::String(s) => Some(s),
                CommonValue::Bool(b) => Some(b),
                CommonValue::Null => None,
                CommonValue::BigInt(bi) => Some(bi),
                CommonValue::Float(f) => Some(f),
            };
            acc.push(p);
            acc
        });

        let feature = self.session.execute_unpaged(&prepare, real_param);
        let query_result = match self.rt.block_on(feature) {
            Ok(ok) => Ok(ok),
            Err(err) => create_error(COMMON_CONN_ERROR_CATEGORY, 
                COMMAND_RUN_ERROR, 
                err.to_string()).as_error()
        }?;

        if typ.len() <= 0 {
            return Ok(result);
        }
        
        let rows = match query_result.into_rows_result() {
            Ok(ok) => Ok(ok),
            Err(err) => create_error(COMMON_CONN_ERROR_CATEGORY, 
                RESPONSE_SCAN_ERROR, 
                err.to_string()).as_error()
        }?;

        let mut fetcher = ScyllaFetcher::new(&rows, &typ);

        fetcher.fetch(&mut result).map_err(|e| {
            create_error(COMMON_CONN_ERROR_CATEGORY, 
                RESPONSE_SCAN_ERROR, 
                e.to_string()).as_error::<()>().err().unwrap()
        })?;

        Ok(result)
    }

    fn get_current_time(&mut self) -> Result<std::time::Duration, Box<dyn Error>> {
        let ret = self.execute("SELECT CAST(toUnixTimestamp(now()) AS BIGINT) AS unix_timestamp  FROM system.local", &[])?;

        if ret.cols_data.len() <= 0 && ret.cols_data[0].len() <= 0 {
            return create_error(COMMON_CONN_ERROR_CATEGORY, 
                RESPONSE_SCAN_ERROR, 
                "not exists now return data".to_string()).as_error();
        }

        let data = match ret.cols_data[0][0] {
            CommonValue::BigInt(bi) => bi,
            CommonValue::Int(i) => i as i64,
            _ => 0
        };

        Ok(std::time::Duration::from_secs(data as u64))
    }
}