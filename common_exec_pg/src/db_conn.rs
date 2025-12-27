use std::collections::HashMap;
use std::time::Duration;
use postgres::Row;
use postgres::types::ToSql;
use postgres::types::Type;
use common_err::{CommonError, gen::CommonDefaultErrorKind};
use common_pair_exec::{PairExecutor, PairValueEnum};

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

    pub(crate) fn new(user : &'_ str, password : &'_ str, addr : &'_ str, name : &'_ str) -> Result<Self, CommonError> {
        let url = Self::create_pg_url(user, password, addr, name);

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
        let ret = self.execute_pair("SELECT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint  AS unix_timestamp", &PairValueEnum::Null).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::ExecuteFail, "get timestamp failed", e)
        })?;

        if let PairValueEnum::Map(m) = ret {
            if let Some(PairValueEnum::Array(data)) = m.get("unix_timestamp") {
                if let PairValueEnum::BigInt(unix_data) = data[0] {
                    Ok(std::time::Duration::from_millis(unix_data as u64))
                }
                else {
                    CommonError::new(&CommonDefaultErrorKind::NotMatchArgs,
                                     "no unix_timestamp value, cols is not int").to_result()
                }
            }
            else {
                CommonError::new(&CommonDefaultErrorKind::NoData,
                                 "no unix_timestamp value, unix_timestamp not exists").to_result()
            }
        }
        else {
            CommonError::new(&CommonDefaultErrorKind::NoData,
                             "no data").to_result()
        }
    }

    fn run_execute_query(&mut self, query : &'_ str, param : Vec<&(dyn ToSql + Sync)>) -> Result<Vec<Row>, CommonError> {
        match self.client.query(query, param.as_slice()) {
            Ok(ok) => Ok(ok),
            Err(err) => CommonError::new(&CommonDefaultErrorKind::InvalidApiCall,
                                         format!("PostgresConnection, [query:{:.1024},dbErr:{}]", query, err.to_string())).to_result()
        }
    }
}

impl PairExecutor for PostgresConnection {
    fn execute_pair(&mut self, query: &'_ str, param: &PairValueEnum) -> Result<PairValueEnum, CommonError> {
        let p = if let PairValueEnum::Array(a) = &param {
            Ok(a.as_slice())
        } else if param == &PairValueEnum::Null {
            const ZERO_ARRAY : [PairValueEnum;0] = [];
            Ok(&ZERO_ARRAY as &[PairValueEnum])
        }else {
            CommonError::new(&CommonDefaultErrorKind::InvalidApiCall, "not support type").to_result()
        }?;

        let pg_param  = convert_common_pair_value_to_pg_param(p)?;

        let rows = self.run_execute_query(query, pg_param).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::ExecuteFail, "run_execute_query failed", e)
        })?;

        if rows.len() <= 0 {
            return Ok(PairValueEnum::Null);
        }

        let row_len = rows.len();
        if row_len <= 0 {
            return Ok(PairValueEnum::Null);
        }
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

        Ok(PairValueEnum::Map(map))
    }

    fn get_current_time(&mut self) -> Result<Duration, CommonError> {
        self.get_current_duration()
    }
}