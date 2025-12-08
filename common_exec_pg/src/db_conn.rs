use std::collections::HashMap;
use std::time::Duration;
use postgres::Row;
use common_relational_exec::{RelationalExecutor, RelationalValue, RelationalExecuteResultSet, RelationalExecutorInfo};
use postgres::types::ToSql;
use postgres::types::Type;
use common_err::{CommonError, gen::CommonDefaultErrorKind};
use common_pair_exec::{PairExecuteRet, PairExecutor, PairValueEnum};

pub struct PostgresConnection {
    client : postgres::Client
}

macro_rules! get_pg_data {
    ($row_col : expr, $idx : expr, $origin_t : ty, $common_ident :ident, $common_t :ident) => {
        {
            let opt : Option<$origin_t> = $row_col.get($idx);
            match opt {
                None => $common_ident::Null,
                Some(s) => $common_ident::$common_t(s)
            }
        }
    };
}

fn convert_common_value_to_pg_param(param : &'_ [RelationalValue]) -> Result<Vec<&(dyn ToSql + Sync)>, CommonError> {
    param.iter().map(| x | {
        let convert: Result<&(dyn ToSql + Sync), CommonError> = match x {
            RelationalValue::BigInt(i) => Ok(i),
            RelationalValue::Int(i) => Ok(i),
            RelationalValue::Null => Ok(&Option::<i64>::None),
            RelationalValue::Double(f) => Ok(f),
            RelationalValue::Bin(v) => Ok(v),
            RelationalValue::String(t) => Ok(t),
            _ => {
                CommonError::new(&CommonDefaultErrorKind::ParsingFail, 
                                 format!("convert_common_value_to_pg_param - not support type({:?}), return null", x)).to_result()
                
            }
        };
        convert
    }).collect::<Result<Vec<&(dyn ToSql + Sync)>, CommonError>>()
}

fn convert_common_pair_value_to_pg_param(param : &'_ [PairValueEnum]) -> Result<Vec<&(dyn ToSql + Sync)>, CommonError> {
    param.iter().map(| x | {
        let convert: Result<&(dyn ToSql + Sync), CommonError> = match x {
            PairValueEnum::BigInt(i) => Ok(i),
            PairValueEnum::Int(i) => Ok(i),
            PairValueEnum::Null => Ok(&Option::<i64>::None),
            PairValueEnum::Double(f) => Ok(f),
            PairValueEnum::Bin(v) => Ok(v),
            PairValueEnum::String(t) => Ok(t),
            _ => {
                CommonError::new(&CommonDefaultErrorKind::ParsingFail,
                                 format!("convert_common_value_to_pg_param - not support type({:?}), return null", x)).to_result()

            }
        };
        convert
    }).collect::<Result<Vec<&(dyn ToSql + Sync)>, CommonError>>()
}

impl PostgresConnection {
    fn create_pg_url(username : &'_ str, password : &'_ str, addr : &'_ str, db_name : &'_ str) -> String {
        format!("postgresql://{username}:{password}@{addr}/{db_name}?connect_timeout=60")
    }

    pub(crate) fn new(info : RelationalExecutorInfo) -> Result<Self, CommonError> {
        let url = Self::create_pg_url(&info.user, &info.password, &info.addr, &info.name);

        let conn = match postgres::Client::connect(url.as_str(), postgres::NoTls) {
            Ok(ok) => Ok(ok),
            Err(err) => CommonError::new(&CommonDefaultErrorKind::ConnectFail, 
                                         format!("PostgresConnection - new - {}", err.to_string())).to_result()
        }?;

        Ok(PostgresConnection {
            client : conn
        })
    }
    fn get_current_duration(&mut self) -> Result<std::time::Duration, CommonError> {
        let ret = self.execute("SELECT EXTRACT(EPOCH FROM NOW() * 1000)::bigint AS unix_timestamp", &[])?;

        if ret.cols_data.len() <= 0 && ret.cols_data[0].len() <= 0 {
            return CommonError::new(&CommonDefaultErrorKind::NoData,  "PostgresConnection - get_current_time - not exists now return data").to_result();
        }

        let data = match ret.cols_data[0][0] {
            RelationalValue::BigInt(bi) => bi,
            RelationalValue::Int(i) => i as i64,
            _ => 0
        };

        Ok(std::time::Duration::from_millis(data as u64))
    }

    fn run_execute_query(&mut self, query : &'_ str, param : Vec<&(dyn ToSql + Sync)>) -> Result<Vec<Row>, CommonError> {
        match self.client.query(query, param.as_slice()) {
            Ok(ok) => Ok(ok),
            Err(err) => CommonError::new(&CommonDefaultErrorKind::InvalidApiCall,
                                         format!("PostgresConnection - execute - {}", err.to_string())).to_result()
        }
    }
}

impl RelationalExecutor<RelationalValue> for PostgresConnection {
    fn execute(&mut self, query : &'_ str, param : &'_ [RelationalValue]) -> Result<RelationalExecuteResultSet, CommonError> {
        let pg_param  = convert_common_value_to_pg_param(param)?;

        let rows = self.run_execute_query(query, pg_param)?;

        let mut ret = RelationalExecuteResultSet::default();

        if rows.len() <= 0 {
            return Ok(ret);
        }
        let mut cols_t = Vec::with_capacity(rows[0].columns().len());

        for col in rows[0].columns() {
            cols_t.push(col.type_());
            ret.cols_name.push(col.name().to_string());
        }

        for row in &rows {
            let mut col_data = Vec::with_capacity(cols_t.len());

            for col_idx in 0..cols_t.len() {
                let d = match cols_t[col_idx] {
                    &Type::BOOL => Ok(get_pg_data!(row, col_idx, bool, RelationalValue, Bool)),
                    &Type::CHAR | &Type::VARCHAR | &Type::TEXT => Ok(get_pg_data!(row, col_idx, String, RelationalValue, String)),
                    &Type::FLOAT4 | &Type::FLOAT8 | &Type::NUMERIC => Ok(get_pg_data!(row, col_idx, f64, RelationalValue, Double)),
                    &Type::INT2 | &Type::INT4 =>Ok(get_pg_data!(row, col_idx, i32, RelationalValue, Int)),
                    &Type::INT8 => Ok(get_pg_data!(row, col_idx, i64, RelationalValue, BigInt)),
                    &Type::BYTEA => Ok(get_pg_data!(row, col_idx, Vec<u8>, RelationalValue, Bin)),
                    _ => {
                        CommonError::new(&CommonDefaultErrorKind::ParsingFail,  
                                         format!("PostgresConnection - execute - not support this type({}), return NULL", cols_t[col_idx])).to_result()
                    }
                }?;

                col_data.push(d);
            }
            ret.cols_data.push(col_data);
        }

        Ok(ret)
    }

    fn get_current_time(&mut self) -> Result<std::time::Duration, CommonError> {
        self.get_current_duration()
    }
}

impl PairExecutor for PostgresConnection {
    fn execute_pair(&mut self, query: &'_ str, param: &PairExecuteRet) -> Result<PairExecuteRet, CommonError> {
        let p = if let PairValueEnum::Array(a) = &param.1 {
            Ok(a.as_slice())
        } else {
            CommonError::new(&CommonDefaultErrorKind::InvalidApiCall, "not support type").to_result()
        }?;

        let pg_param  = convert_common_pair_value_to_pg_param(p)?;

        let rows = self.run_execute_query(query, pg_param)?;
        let mut ret = PairExecuteRet::default();

        if rows.len() <= 0 {
            return Ok(ret);
        }

        let row_len = rows.len();
        let mut map = HashMap::new();

        let cols = rows[0].columns();
        for col_idx in 0..cols.len() {
            let ty = cols[col_idx].type_();
            let col_name = cols[col_idx].name().to_string();
            let mut v = Vec::new();

            for row_idx in 0..row_len {
                let d = match ty {
                    &Type::BOOL => Ok(get_pg_data!(rows[row_idx], col_idx, bool, PairValueEnum, Bool)),
                    &Type::CHAR | &Type::VARCHAR | &Type::TEXT => Ok(get_pg_data!(rows[row_idx], col_idx, String, PairValueEnum, String)),
                    &Type::FLOAT4 | &Type::FLOAT8 | &Type::NUMERIC => Ok(get_pg_data!(rows[row_idx], col_idx, f64, PairValueEnum, Double)),
                    &Type::INT2 | &Type::INT4 =>Ok(get_pg_data!(rows[row_idx], col_idx, i32, PairValueEnum, Int)),
                    &Type::INT8 => Ok(get_pg_data!(rows[row_idx], col_idx, i64, PairValueEnum, BigInt)),
                    &Type::BYTEA => Ok(get_pg_data!(rows[row_idx], col_idx, Vec<u8>, PairValueEnum, Bin)),
                    _ => {
                        CommonError::new(&CommonDefaultErrorKind::ParsingFail,
                                         format!("PostgresConnection - execute - not support this type({}), return NULL", ty)).to_result()
                    }
                }?;

                v.push(d);
            }
            map.insert(col_name.clone(), PairValueEnum::Array(v));

        }

        ret.1 = PairValueEnum::Map(map);
        Ok(ret)
    }

    fn get_current_time(&mut self) -> Result<Duration, CommonError> {
        self.get_current_duration()
    }
}