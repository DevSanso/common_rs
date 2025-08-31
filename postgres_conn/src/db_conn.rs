use std::error::Error;

use common_conn::CommonSqlExecuteResultSet;
use common_conn::CommonSqlTxConnection;
use postgres::types::ToSql;
use postgres::types::Type;

use common_core::err::*;
use common_conn::{CommonSqlConnection, CommonSqlConnectionInfo, CommonValue};
use common_conn::err::*;

pub struct PostgresConnection {
    client : postgres::Client   
} 

macro_rules! get_pg_data {
    ($row_col : expr, $idx : expr, $origin_t : ty, $common_ident :ident, $common_t :ident) => {
        {
            let opt : Option<$origin_t> = $row_col.get($idx);
            match opt {
                None => CommonValue::Null,
                Some(s) => $common_ident::$common_t(s)
            }
        }
    };
}

fn convert_common_value_to_pg_param(param : &'_ [CommonValue]) -> Result<Vec<&(dyn ToSql + Sync)>, Box<dyn Error>> {
    param.iter().map(| x | {
        let convert: Result<&(dyn ToSql + Sync), Box<dyn Error>> = match x {
            CommonValue::BigInt(i) => Ok(i),
            CommonValue::Int(i) => Ok(i),
            CommonValue::Null => Ok(&Option::<i64>::None),
            CommonValue::Double(f) => Ok(f),
            CommonValue::Binrary(v) => Ok(v),
            CommonValue::String(t) => Ok(t),
            _ => {
                create_error(COMMON_ERROR_CATEGORY, 
                    CRITICAL_ERROR, 
                    format!("not support type({:?}), return null", x), None).as_error()
            }
        };
        convert
    }).collect::<Result<Vec<&(dyn ToSql + Sync)>, Box<dyn Error>>>()
}

impl PostgresConnection {
    fn create_pg_url(username : &'_ str, password : &'_ str, addr : &'_ str, db_name : &'_ str) -> String {
        format!("postgresql://{username}:{password}@{addr}/{db_name}?connect_timeout=60")
    }

    pub(crate) fn new(info : CommonSqlConnectionInfo) -> Result<Self, Box<dyn Error>> {
        let url = Self::create_pg_url(&info.user, &info.password, &info.addr, &info.db_name);
        
        let conn = match postgres::Client::connect(url.as_str(), postgres::NoTls) {
            Ok(ok) => Ok(ok),
            Err(err) => create_error(COMMON_CONN_ERROR_CATEGORY, 
                GET_CONNECTION_FAILED_ERROR, 
                "".to_string(), Some(Box::new(err))).as_error()
        }?;

        Ok(PostgresConnection {
            client : conn
        })
    }
}

impl CommonSqlConnection for PostgresConnection {
    fn execute(&mut self, query : &'_ str, param : &'_ [CommonValue]) -> Result<common_conn::CommonSqlExecuteResultSet, Box<dyn std::error::Error>> {
        let pg_param  = convert_common_value_to_pg_param(param)?;

        let rows = match self.client.query(query, pg_param.as_slice()) {
            Ok(ok) => Ok(ok),
            Err(err) => create_error(COMMON_CONN_ERROR_CATEGORY, 
                COMMAND_RUN_ERROR, 
                "".to_string(), Some(Box::new(err))).as_error()
        }?;



        let mut ret = CommonSqlExecuteResultSet::default();

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
                    &Type::BOOL => Ok(get_pg_data!(row, col_idx, bool, CommonValue, Bool)),
                    &Type::CHAR | &Type::VARCHAR | &Type::TEXT => Ok(get_pg_data!(row, col_idx, String, CommonValue, String)),
                    &Type::FLOAT4 | &Type::FLOAT8 | &Type::NUMERIC => Ok(get_pg_data!(row, col_idx, f64, CommonValue, Double)),
                    &Type::INT2 | &Type::INT4 =>Ok(get_pg_data!(row, col_idx, i32, CommonValue, Int)),
                    &Type::INT8 => Ok(get_pg_data!(row, col_idx, i64, CommonValue, BigInt)),
                    &Type::BYTEA => Ok(get_pg_data!(row, col_idx, Vec<u8>, CommonValue, Binrary)),
                    _ => {
                        create_error(COMMON_CONN_ERROR_CATEGORY, 
                            RESPONSE_SCAN_ERROR, 
                            format!("not support this type({}), return NULL", cols_t[col_idx]), None).as_error()
                    }
                }?;

                col_data.push(d);
            }
            ret.cols_data.push(col_data);
        }

        Ok(ret)
    }

    fn get_current_time(&mut self) -> Result<std::time::Duration, Box<dyn Error>> {
        let ret = self.execute("SELECT EXTRACT(EPOCH FROM NOW())::bigint AS unix_timestamp;", &[])?;

        if ret.cols_data.len() <= 0 && ret.cols_data[0].len() <= 0 {
            return create_error(COMMON_CONN_ERROR_CATEGORY, 
                RESPONSE_SCAN_ERROR, 
                "not exists now return data".to_string(), None).as_error();
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

impl CommonSqlTxConnection for PostgresConnection {
    fn execute_tx(&mut self, query : &'_ str, params : &'_[&'_ [CommonValue]]) -> Result<(), Box<dyn Error>> {
        let mut tx = match self.client.transaction() {
            Ok(ok) => Ok(ok),
            Err(e) => create_error(COMMON_CONN_ERROR_CATEGORY, TRANSACTION_CALL_ERROR
                , "failed tx call".to_string(), Some(Box::new(e))).as_error()
        }?;

        let mut ret = Ok(0);

        for param in params {
            let p = convert_common_value_to_pg_param(param)?;
            let exec_ret = tx.execute(query, p.as_slice());

            ret = exec_ret;
            if ret.is_err() {break}
        }

        if ret.is_err() {
            let _ = tx.rollback();

            return create_error(COMMON_CONN_ERROR_CATEGORY, TRANSACTION_CALL_ERROR,
                "execute failed".to_string(), Some(Box::new(ret.unwrap_err()))).as_error();
        }
        else {
            let _ = tx.commit();
        }

        Ok(())
    }
}