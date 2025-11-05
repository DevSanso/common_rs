use std::error::Error;

use duckdb;
use duckdb::types::ToSql;
use duckdb::arrow::datatypes::DataType;

use common_relational_exec::{RelationalExecutor, RelationalValue, RelationalExecuteResultSet, RelationalExecutorInfo};
use common_core::utils::types::SimpleError;

pub struct DuckDBConnection {
    client : duckdb::Connection
}

fn convert_common_value_to_duckdb_param(param : &'_ [RelationalValue]) -> Result<Vec<&dyn ToSql>, Box<dyn Error>> {
    param.iter().map(| x | {
        let convert: Result<&dyn ToSql, Box<dyn Error>> = match x {
            RelationalValue::BigInt(i) => Ok(i),
            RelationalValue::Int(i) => Ok(i),
            RelationalValue::Null => Ok(&Option::<i64>::None),
            RelationalValue::Double(f) => Ok(f),
            RelationalValue::Bin(v) => Ok(v),
            RelationalValue::String(t) => Ok(t),
            _ => SimpleError {msg : format!("not support type({:?}), return null", x)}.into_result()

        };
        convert
    }).collect::<Result<Vec<&dyn ToSql>, Box<dyn Error>>>()
}

impl DuckDBConnection {
    pub(crate) fn new(info : RelationalExecutorInfo) -> Result<Self, Box<dyn Error>> {
        if info.addr == "" {
            let c = DuckDBConnection { client:  duckdb::Connection::open_in_memory()?};
            Ok(c)
        } else {
            let c = DuckDBConnection { client:  duckdb::Connection::open(info.addr)?};
            Ok(c)
        }
    }
}

impl RelationalExecutor<RelationalValue> for DuckDBConnection {
    fn execute(&mut self, query : &'_ str, param : &'_ [RelationalValue]) -> Result<RelationalExecuteResultSet, Box<dyn Error>> {
        let mut prepare = self.client.prepare(query).map_err(|x| {
            SimpleError {msg : format!("DuckDBConnection - execute - {}", x.to_string())}.into_result::<()>().unwrap_err()
        })?;

        let duck_param  = convert_common_value_to_duckdb_param(param)?;

        let mut ret = RelationalExecuteResultSet::default();

        let col_count = prepare.column_count();
        let schema = prepare.schema();

        ret.cols_name = prepare.column_names();
        ret.cols_data = Vec::with_capacity(10);

        let mut rows = prepare.query(duck_param.as_slice()).map_err(|x| {
            SimpleError {msg : format!("DuckDBConnection - execute,query - {}", x.to_string())}.into_result::<()>().unwrap_err()
        })?;

        loop  {
            let row = rows.next();
            if row.is_err() {
                let e = row.err().unwrap();
                return SimpleError {msg : format!("DuckDBConnection - execute,next - {}", e.to_string())}.into_result();
            }

            let r = row.unwrap();
            if r.is_none() {break;}

            let mut common_row = Vec::new();

            let r_data = r.unwrap();

            for idx in 0..col_count {
                let data = match schema.field(idx).data_type() {
                    DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                        let conv : i64 = r_data.get(idx).unwrap();
                        Ok(RelationalValue::BigInt(conv))
                    },
                    DataType::Utf8 => {
                        let conv : Vec<u8> = r_data.get(idx).unwrap();
                        Ok(RelationalValue::String(String::from_utf8(conv).unwrap()))
                    },
                    DataType::Float16 | DataType::Float32  => {
                        let conv : f32 = r_data.get(idx).unwrap();
                        Ok(RelationalValue::Float(conv))
                    },
                    DataType::Float64 => {
                        let conv : f64 = r_data.get(idx).unwrap();
                        Ok(RelationalValue::Double(conv))
                    },
                    DataType::Null => {
                        Ok(RelationalValue::Null)
                    },
                    DataType::Binary => {
                        let conv : Vec<u8> = r_data.get(idx).unwrap();
                        Ok(RelationalValue::Bin(conv))
                    },
                    _ => SimpleError {msg : "DuckDBConnection - \
                        execute,cast - not exists col type data".to_string()}.into_result()
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
                SimpleError {msg : format!("DuckDBConnection - get_current_time - {}", x.to_string())}.into_result::<()>().unwrap_err()
            })?;

        Ok(std::time::Duration::from_secs(data as u64))
    }
    
}