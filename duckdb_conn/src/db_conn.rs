use std::error::Error;

use duckdb;
use duckdb::types::ToSql;
use duckdb::arrow::datatypes::DataType;

use common_core::err::*;
use common_conn::{CommonSqlConnection, CommonSqlConnectionInfo, CommonValue};
use common_conn::err::*;

pub struct DuckDBConnection {
    client : duckdb::Connection
}

impl DuckDBConnection {
    pub(crate) fn new(info : CommonSqlConnectionInfo) -> Result<Self, Box<dyn Error>> {
        if info.addr == "" {
            let c = DuckDBConnection { client:  duckdb::Connection::open_in_memory()?};
            Ok(c)
        } else {
            let c = DuckDBConnection { client:  duckdb::Connection::open(info.addr)?};
            Ok(c)
        }
    }
}

impl CommonSqlConnection for DuckDBConnection {
    fn execute(&mut self, query : &'_ str, param : &'_ [CommonValue]) -> Result<common_conn::CommonSqlExecuteResultSet, Box<dyn std::error::Error>> {
        let mut prepare = self.client.prepare(query).map_err(|x| {
            create_error(COMMON_ERROR_CATEGORY, 
                CONNECTION_API_CALL_ERROR, 
                x.to_string()).as_error::<()>().err().unwrap()
        })?;

        let duck_param  = param.iter().map(| x | {
            let convert: Result<&(dyn ToSql), Box<dyn Error>> = match x {
                CommonValue::BigInt(i) => Ok(i),
                CommonValue::Int(i) => Ok(i),
                CommonValue::Null => Ok(&Option::<i64>::None),
                CommonValue::Double(f) => Ok(f),
                CommonValue::Binrary(v) => Ok(v),
                CommonValue::String(t) => Ok(t),
                _ => create_error(COMMON_ERROR_CATEGORY, 
                        CRITICAL_ERROR, 
                        format!("not support type({:?}), return null", x)).as_error()
                
            };
            convert
        }).collect::<Result<Vec<&(dyn ToSql)>, Box<dyn Error>>>()?;

        let mut ret = common_conn::CommonSqlExecuteResultSet::default();
  
        let col_count = prepare.column_count();
        let schema = prepare.schema();

        ret.cols_name = prepare.column_names();
        ret.cols_data = Vec::with_capacity(10);

        let mut rows = prepare.query(duck_param.as_slice()).map_err(|x| {
            create_error(COMMON_ERROR_CATEGORY, 
                COMMAND_RUN_ERROR, 
                x.to_string()).as_error::<()>().err().unwrap()
        })?;

        loop  {
            let row = rows.next();
            if row.is_err() {
                return create_error(COMMON_ERROR_CATEGORY, 
                    CONNECTION_API_CALL_ERROR, 
                    row.err().unwrap().to_string()).as_error();
            }

            let r = row.unwrap();
            if r.is_none() {break;}

            let mut common_row = Vec::new();

            let r_data = r.unwrap();

            for idx in 0..col_count {
                let data = match schema.field(idx).data_type() {
                    DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                        let conv : i64 = r_data.get(idx).unwrap();
                        Ok(CommonValue::BigInt(conv))
                    },
                    DataType::Utf8 => {
                        let conv : Vec<u8> = r_data.get(idx).unwrap();
                        Ok(CommonValue::String(String::from_utf8(conv).unwrap()))
                    },
                    DataType::Float16 | DataType::Float32  => {
                        let conv : f32 = r_data.get(idx).unwrap();
                        Ok(CommonValue::Float(conv))
                    },
                    DataType::Float64 => {
                        let conv : f64 = r_data.get(idx).unwrap();
                        Ok(CommonValue::Double(conv))
                    },
                    DataType::Null => {
                        Ok(CommonValue::Null)
                    },
                    DataType::Binary => {
                        let conv : Vec<u8> = r_data.get(idx).unwrap();
                        Ok(CommonValue::Binrary(conv))
                    },
                    _ => create_error(COMMON_ERROR_CATEGORY, 
                        NO_SUPPORT_ERROR, 
                        "not exists col type data".to_string()).as_error()
                };

                common_row.push(data?);
            }

            ret.cols_data.push(common_row);
        } 

        Ok(ret)
    }

    fn get_current_time(&mut self) -> Result<std::time::Duration, Box<dyn Error>> {
        let data : i64 = self.client.query_row(
            "SELECT CAST(extract(epoch FROM current_timestamp) AS INTEGER) AS unix_time", [], |r| r.get(0))
            .map_err(|x| {
                create_error(COMMON_CONN_ERROR_CATEGORY, 
                    RESPONSE_SCAN_ERROR, 
                    x.to_string()).as_error::<()>().err().unwrap()
            })?;
        
        Ok(std::time::Duration::from_secs(data as u64))
    }
}