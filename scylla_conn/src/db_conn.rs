mod utils;

use std::error::Error;

use scylla::batch::Batch;
use scylla::Session;
use scylla::serialize::value::SerializeValue;
use tokio::runtime::{Builder, Runtime};

use common_core::err::{create_error};
use common_conn::{CommonSqlConnection, CommonSqlConnectionInfo, CommonSqlExecuteResultSet, CommonSqlTxConnection, CommonValue};
use common_conn::err::*;
use scylla::SessionBuilder;
use crate::db_conn::utils::{ScyllaBatchParams, ScyllaFetcherRow};
pub struct ScyllaCommonSqlConnection {
    session : Session,
    rt : Runtime
}

fn convert_common_value_to_scylla_param(param : &'_ [CommonValue]) -> Vec<Option<&dyn SerializeValue>> {
    param.iter().fold(Vec::<Option<&dyn SerializeValue>>::new(), |mut acc,x | {
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
    })
}

impl ScyllaCommonSqlConnection {
    pub(crate) fn new(infos : Vec<CommonSqlConnectionInfo>) -> Result<Self, Box<dyn Error>> {
        if infos.len() <= 0 {
            return create_error(COMMON_CONN_ERROR_CATEGORY, 
                GET_CONNECTION_FAILED_ERROR, 
                "scylla connection info array size of zero".to_string(), None).as_error();
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
                "".to_string(), Some(Box::new(err))).as_error()
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
                "".to_string(), Some(Box::new(err))).as_error()
        }?;

        let mut result = CommonSqlExecuteResultSet::default();

        let mut typ = Vec::new();
        for col in prepare.get_result_set_col_specs() {
            result.cols_name.push(col.name().to_string());
            typ.push(col.typ());
        }

        let real_param = convert_common_value_to_scylla_param(param);

        let feature = self.session.execute_unpaged(&prepare, real_param);
        let query_result = match self.rt.block_on(feature) {
            Ok(ok) => Ok(ok),
            Err(err) => create_error(COMMON_CONN_ERROR_CATEGORY, 
                COMMAND_RUN_ERROR, 
                "".to_string(), Some(Box::new(err))).as_error()
        }?;

        if typ.len() <= 0 {
            return Ok(result);
        }
        
        let rows = match query_result.into_rows_result() {
            Ok(ok) => Ok(ok),
            Err(err) => create_error(COMMON_CONN_ERROR_CATEGORY, 
                RESPONSE_SCAN_ERROR, 
                "".to_string(), Some(Box::new(err))).as_error()
        }?;

        let mut row_iter = match rows.rows::<ScyllaFetcherRow>() {
            Ok(ok) => Ok(ok),
            Err(err) => create_error(COMMON_CONN_ERROR_CATEGORY, 
                RESPONSE_SCAN_ERROR, 
                "".to_string(), Some(Box::new(err))).as_error()
        }?;

        while let Some(r) = row_iter.next() {
            let mut convert_row = match r {
                Ok(ok) => Ok(ok),
                Err(err) => create_error(COMMON_CONN_ERROR_CATEGORY, 
                    RESPONSE_SCAN_ERROR, 
                    "".to_string(), Some(Box::new(err))).as_error()
            }?;

            let chk_err = convert_row.get_error();
            if chk_err.is_err() {
                return create_error(COMMON_CONN_ERROR_CATEGORY, 
                    RESPONSE_SCAN_ERROR, 
                    "".to_string(), Some(chk_err.err().unwrap())).as_error();
            }
            let col_data = convert_row.clone_col();

            if col_data.len() != result.cols_name.len() {
                return create_error(COMMON_CONN_ERROR_CATEGORY, 
                    RESPONSE_SCAN_ERROR, 
                    format!("data len : {} != col count : {}", col_data.len(), result.cols_name.len()), None).as_error();
            } 

            result.cols_data.push(col_data);
        }

        Ok(result)
    }

    fn get_current_time(&mut self) -> Result<std::time::Duration, Box<dyn Error>> {
        let ret = self.execute("SELECT CAST(toUnixTimestamp(now()) AS BIGINT) AS unix_timestamp  FROM system.local", &[])?;

        if ret.cols_data.len() <= 0 && ret.cols_data[0].len() <= 0 {
            return create_error(COMMON_CONN_ERROR_CATEGORY, 
                RESPONSE_SCAN_ERROR, 
                "".to_string(), None).as_error();
        }

        let data = match ret.cols_data[0][0] {
            CommonValue::BigInt(bi) => bi,
            CommonValue::Int(i) => i as i64,
            _ => 0
        };

        Ok(std::time::Duration::from_secs(data as u64))
    }
    
    fn trans(self) -> Result<Box<dyn common_conn::CommonSqlTxConnection>, Box<dyn Error>> {
        Ok(Box::new(self))
    }
}

impl CommonSqlTxConnection for ScyllaCommonSqlConnection {
    fn execute_tx(&mut self, query : &'_ str, params : &'_[&'_ [CommonValue]]) -> Result<(), Box<dyn Error>> {
        let mut batch = Batch::new(scylla::batch::BatchType::Logged);
        let mut param_size = 0;
        for i in 0..params.len() {
            batch.append_statement(query);
            param_size = params[i].len();
        }

        let mut real_batch_vec = Vec::with_capacity(params.len());
        for v in params {
            let mut copyd = Vec::with_capacity(param_size);
            copyd.extend_from_slice(v);
            real_batch_vec.push(copyd);
        }
        
        let param_wrap = ScyllaBatchParams::new(real_batch_vec);

        let future = self.session.batch(&batch, param_wrap.as_batch_value_iter());
        let _ = self.rt.block_on(future).map_err(|x| {
            create_error(COMMON_CONN_ERROR_CATEGORY, TRANSACTION_CALL_ERROR,
                 "batch thread run failed".to_string(), None).as_error::<()>().err().unwrap()
        })?;
        Ok(())
    }
}