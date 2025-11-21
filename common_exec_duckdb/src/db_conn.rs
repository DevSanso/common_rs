use duckdb;
use duckdb::types::ToSql;
use duckdb::arrow::datatypes::DataType;
use common_relational_exec::{RelationalExecutor, RelationalValue, RelationalExecuteResultSet, RelationalExecutorInfo};
use common_err::{CommonError, gen::CommonDefaultErrorKind};

pub struct DuckDBConnection {
    client : duckdb::Connection
}

fn convert_common_value_to_duckdb_param(param : &'_ [RelationalValue]) -> Result<Vec<&dyn ToSql>, CommonError> {
    param.iter().map(| x | {
        let convert: Result<&dyn ToSql, CommonError> = match x {
            RelationalValue::BigInt(i) => Ok(i),
            RelationalValue::Int(i) => Ok(i),
            RelationalValue::Null => Ok(&Option::<i64>::None),
            RelationalValue::Double(f) => Ok(f),
            RelationalValue::Bin(v) => Ok(v),
            RelationalValue::String(t) => Ok(t),
            _ => CommonError::new(&CommonDefaultErrorKind::NoSupport, format!("not support type({:?}), return null", x)).to_result()

        };
        convert
    }).collect::<Result<Vec<&dyn ToSql>, CommonError>>()
}

impl DuckDBConnection {
    pub(crate) fn new(info : RelationalExecutorInfo) -> Result<Self, CommonError> {
        if info.addr == "" {
            let c = DuckDBConnection { client:  duckdb::Connection::open_in_memory().map_err(|e| {
                CommonError::new(&CommonDefaultErrorKind::InvalidApiCall, format!("Cannot open DuckDBConnection: {}", e))
            })?};
            Ok(c)
        } else {
            let c = DuckDBConnection { client:  duckdb::Connection::open(info.addr).map_err(|e| {
                CommonError::new(&CommonDefaultErrorKind::InvalidApiCall, format!("Cannot open DuckDBConnection: {}", e))
            })?};
            Ok(c)
        }
    }
}

impl RelationalExecutor<RelationalValue> for DuckDBConnection {
    fn execute(&mut self, query : &'_ str, param : &'_ [RelationalValue]) -> Result<RelationalExecuteResultSet, CommonError> {
        let mut prepare = self.client.prepare(query).map_err(|x| {
            CommonError::new(&CommonDefaultErrorKind::InvalidApiCall, format!("DuckDBConnection - execute - {}", x.to_string()))
        })?;

        let duck_param  = convert_common_value_to_duckdb_param(param)?;

        let mut ret = RelationalExecuteResultSet::default();

        let col_count = prepare.column_count();
        let schema = prepare.schema();

        ret.cols_name = prepare.column_names();
        ret.cols_data = Vec::with_capacity(10);

        let mut rows = prepare.query(duck_param.as_slice()).map_err(|x| {
            CommonError::new(&CommonDefaultErrorKind::InvalidApiCall, format!("DuckDBConnection - execute,query - {}", x.to_string()))
        })?;

        loop  {
            let row = rows.next();
            if row.is_err() {
                let e = row.err().unwrap();
                return CommonError::new(&CommonDefaultErrorKind::InvalidApiCall, 
                                        format!("DuckDBConnection - execute,next - {}", e.to_string())).to_result()
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
                    _ => CommonError::new(&CommonDefaultErrorKind::ParsingFail, "DuckDBConnection - \
                                          execute,cast - not exists col type data").to_result()
                };

                common_row.push(data?);
            }

            ret.cols_data.push(common_row);
        }

        Ok(ret)
    }

    fn get_current_time(&mut self) -> Result<std::time::Duration, CommonError> {
        let data : i64 = self.client.query_row(
            "SELECT (epoch(now()) * 1000)::BIGINT AS unix_ms", [], |r| r.get(0))
            .map_err(|x| {
                CommonError::new(&CommonDefaultErrorKind::InvalidApiCall, format!("Cannot get current time: {}", x.to_string()))
            })?;

        Ok(std::time::Duration::from_millis(data as u64))
    }
    
}