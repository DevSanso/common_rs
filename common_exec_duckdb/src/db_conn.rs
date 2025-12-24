use std::collections::HashMap;
use std::time::Duration;
use duckdb;
use duckdb::types::ToSql;
use duckdb::arrow::datatypes::DataType;
use common_err::{CommonError, gen::CommonDefaultErrorKind};
use common_pair_exec::{PairExecutor, PairValueEnum};

pub struct DuckDBConnection {
    client : duckdb::Connection
}

macro_rules! get_row_data {
    ($schema:expr, $idx:expr, $r_data:expr, $ret_type:ident) => {
        match $schema.field($idx).data_type() {
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                    let conv : i64 = $r_data.get($idx).unwrap();
                    Ok($ret_type::BigInt(conv))
                },
                DataType::Utf8 => {
                    let conv : Vec<u8> = $r_data.get($idx).unwrap();
                    Ok($ret_type::String(String::from_utf8(conv).unwrap()))
                },
                DataType::Float16 | DataType::Float32  => {
                    let conv : f32 = $r_data.get($idx).unwrap();
                    Ok($ret_type::Float(conv))
                },
                DataType::Float64 => {
                    let conv : f64 = $r_data.get($idx).unwrap();
                    Ok($ret_type::Double(conv))
                },
                DataType::Null => {
                    Ok($ret_type::Null)
                },
                DataType::Binary => {
                    let conv : Vec<u8> = $r_data.get($idx).unwrap();
                    Ok($ret_type::Bin(conv))
                },
                _ => CommonError::new(&CommonDefaultErrorKind::ParsingFail, "DuckDBConnection - \
                                      execute,cast - not exists col type data").to_result()
            }
    };
}

fn convert_pair_value_to_duckdb_param(param : &'_ [PairValueEnum]) -> Result<Vec<&dyn ToSql>, CommonError> {
    param.iter().map(| x | {
        let convert: Result<&dyn ToSql, CommonError> = match x {
            PairValueEnum::BigInt(i) => Ok(i),
            PairValueEnum::Int(i) => Ok(i),
            PairValueEnum::Null => Ok(&Option::<i64>::None),
            PairValueEnum::Double(f) => Ok(f),
            PairValueEnum::Bin(v) => Ok(v),
            PairValueEnum::String(t) => Ok(t),
            _ => CommonError::new(&CommonDefaultErrorKind::NoSupport, format!("not support type({:?}), return null", x)).to_result()

        };
        convert
    }).collect::<Result<Vec<&dyn ToSql>, CommonError>>()
}

impl DuckDBConnection {
    pub(crate) fn new(addr : &'_ str) -> Result<Self, CommonError> {
        if addr == "" {
            let c = DuckDBConnection { client:  duckdb::Connection::open_in_memory().map_err(|e| {
                CommonError::new(&CommonDefaultErrorKind::InvalidApiCall, format!("Cannot open DuckDBConnection: {}", e))
            })?};
            Ok(c)
        } else {
            let c = DuckDBConnection { client:  duckdb::Connection::open(addr).map_err(|e| {
                CommonError::new(&CommonDefaultErrorKind::InvalidApiCall, format!("Cannot open DuckDBConnection: {}", e))
            })?};
            Ok(c)
        }
    }

    fn get_current_duration(&mut self) -> Result<std::time::Duration, CommonError> {
        let ret = self.execute_pair("SELECT (epoch(now()) * 1000)::BIGINT AS unix_ms", &PairValueEnum::Null).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::ExecuteFail, "get timestamp failed", e)
        })?;

        if let PairValueEnum::Map(m) = ret {
            if let Some(PairValueEnum::Array(data)) = m.get("unix_ms") {
                if let PairValueEnum::BigInt(unix_data) = data[0] {
                    Ok(std::time::Duration::from_millis(unix_data as u64))
                }
                else {
                    CommonError::new(&CommonDefaultErrorKind::NotMatchArgs,
                                     "no unix_ms value, cols is not int").to_result()
                }
            }
            else {
                CommonError::new(&CommonDefaultErrorKind::NoData,
                                 "no unix_timestamp value, unix_ms not exists").to_result()
            }
        }
        else {
            CommonError::new(&CommonDefaultErrorKind::NoData,
                             "no data").to_result()
        }
    }

    fn run_query_query(mut prepare : duckdb::Statement, duck_param : Vec<&'_ dyn ToSql>) -> Result<PairValueEnum, CommonError> {
        let mut rows = prepare.query(duck_param.as_slice()).map_err(|x| {
            CommonError::new(&CommonDefaultErrorKind::InvalidApiCall, format!("DuckDBConnection.execute_pair - execute - {}", x.to_string()))
        })?;

        let mut cache = HashMap::new();
        let mut cache_idx_m = HashMap::new();
        let once = std::sync::atomic::AtomicBool::new(true);

        loop {
            let row = rows.next();

            if row.is_err() {
                let e = row.err().unwrap();
                return CommonError::new(&CommonDefaultErrorKind::InvalidApiCall,
                                        format!("DuckDBConnection - execute,next - {}", e.to_string())).to_result()
            }

            if let Some(r) = row.unwrap() {
                let schema = r.as_ref().schema();

                 if once.swap(false, std::sync::atomic::Ordering::Relaxed) {
                    for field in 0..schema.fields.len() {
                        cache.insert((*schema.fields[field].name()).clone(), Vec::<PairValueEnum>::new());
                        cache_idx_m.insert(field, (*schema.fields[field].name()).clone());
                    }
                };

                for idx in 0..schema.fields.len() {
                    let key = cache_idx_m.get(&idx).ok_or_else(|| {
                        CommonError::new(&CommonDefaultErrorKind::NoData, format!("not exists idx : {}", idx))
                    })?;

                    let v = cache.get_mut(key).ok_or_else(|| {
                        CommonError::new(&CommonDefaultErrorKind::NoData, format!("not exists col idx : {}", idx))
                    })?;

                    let data = get_row_data!(schema, idx, r, PairValueEnum)?;

                    v.push(data);
                }
            }
            else {
                break;
            }
        }


        let mut convert_m = HashMap::new();
        for item in cache {
            convert_m.insert(item.0.to_string(), PairValueEnum::Array(item.1));
        }

        if convert_m.is_empty() {
            Ok(PairValueEnum::Null)
        }
        else {
            Ok(PairValueEnum::Map(convert_m))
        }
    }
}

impl PairExecutor for DuckDBConnection {
    fn execute_pair(&mut self, query: &'_ str, param: &PairValueEnum) -> Result<PairValueEnum, CommonError> {
        let mut prepare = self.client.prepare(query).map_err(|x| {
            CommonError::new(&CommonDefaultErrorKind::InvalidApiCall, format!("DuckDBConnection - execute - {}", x.to_string()))
        })?;
        
        let p = if let PairValueEnum::Array(a) = &param {
            Ok(a.as_slice())
        } else if param == &PairValueEnum::Null {
            const ZERO_ARRAY : [PairValueEnum;0] = [];
            Ok(&ZERO_ARRAY as &[PairValueEnum])
        } else {
            CommonError::new(&CommonDefaultErrorKind::InvalidApiCall, "not support type").to_result()
        }?;

        let duck_param  = convert_pair_value_to_duckdb_param(p)?;

        Self::run_query_query(prepare, duck_param).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::InvalidApiCall, "run_query_query failed", e)
        })
    }

    fn get_current_time(&mut self) -> Result<Duration, CommonError> {
        self.get_current_duration()
    }
}