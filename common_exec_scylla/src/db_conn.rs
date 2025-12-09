mod util;

use std::collections::HashMap;
use std::time::Duration;
use scylla::serialize::value::SerializeValue;
use tokio::runtime::{Builder, Runtime};
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::response::query_result::{QueryResult, QueryRowsResult};
use scylla::statement::prepared::PreparedStatement;
use common_err::{CommonError, gen::CommonDefaultErrorKind};
use common_pair_exec::{PairExecutor, PairExecutorInfo, PairValueEnum};
use common_relational_exec::{RelationalExecutorInfo, RelationalExecuteResultSet, RelationalExecutor, RelationalValue};
use util::ScyllaFetcherRow;
use crate::db_conn::util::ScyllaPairFetcherRow;

pub struct ScyllaConnection {
    session : Session,
    rt : Runtime
}

#[derive(Debug,Clone, Default)]
pub struct ScyllaConnInfo {
    pub addr : String,
    pub name : String,
    pub user : String,
    pub password : String,
    pub timeout_sec : u32
}

impl ScyllaConnection {
    pub(crate) fn new(infos : Vec<ScyllaConnInfo>) -> Result<Self, CommonError> {
        if infos.len() <= 0 {
            return CommonError::new(&CommonDefaultErrorKind::NoData, "scylla connection info array size of zero").to_result();
        }

        let mut builder = SessionBuilder::new();

        for info in infos {
            builder = builder
                .known_node(info.addr)
                .user(info.user, info.password)
                .use_keyspace(info.name, false);
        }

        let rt = Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let feature = builder.build();
        let block = rt.block_on(feature);

        match block {
            Ok(ok) => Ok(ScyllaConnection {session : ok, rt}),
            Err(err) => CommonError::new(&CommonDefaultErrorKind::ConnectFail,
                                         format!("ScyllaConnection.new - {}", err)).to_result()
        }
    }
    
    pub fn get_prepare(&mut self, query : &'_ str) -> Result<PreparedStatement, CommonError> {
        let feature = self.session.prepare(query);

        match self.rt.block_on(feature) {
            Ok(ok) => Ok(ok),
            Err(err) => CommonError::new(&CommonDefaultErrorKind::InvalidApiCall,
                                         format!("ScyllaConnection.get_prepare - {}", err)).to_result()
        }
    }
    
    pub fn execute_query(&mut self, prepare : &PreparedStatement, p : &[Option<&dyn SerializeValue>]) -> Result<QueryResult, CommonError> {
        let feature = self.session.execute_unpaged(prepare, p);
        match self.rt.block_on(feature) {
            Ok(ok) => Ok(ok),
            Err(err) => CommonError::new(&CommonDefaultErrorKind::ExecuteFail,
                                         format!("ScyllaConnection.execute_query - execute_query - {}", err)).to_result()
        }
    }
    
    pub fn get_query_row_result(&self, qr : QueryResult) -> Result<QueryRowsResult, CommonError> {
        match qr.into_rows_result() {
            Ok(ok) => Ok(ok),
            Err(err) => CommonError::new(&CommonDefaultErrorKind::InvalidApiCall,
                                         format!("ScyllaConnection.get_query_row_result - query_result - {}", err)).to_result()
        }
    }

    fn get_current_duration(&mut self) -> Result<std::time::Duration, CommonError> {
        let ret = self.execute("SELECT CAST(toUnixTimestamp(now()) AS BIGINT) AS unix_timestamp  FROM system.local", &[])?;

        if ret.cols_data.len() <= 0 && ret.cols_data[0].len() <= 0 {
            return CommonError::new(&CommonDefaultErrorKind::NotMatchArgs,  "ScyllaConnection.get_current_time").to_result();
        }

        let data = match ret.cols_data[0][0] {
            RelationalValue::BigInt(bi) => bi,
            RelationalValue::Int(i) => i as i64,
            _ => 0
        };

        Ok(std::time::Duration::from_millis(data as u64))
    }
}
impl RelationalExecutor<RelationalValue> for ScyllaConnection {
    fn execute(&mut self, query : &'_ str, param : &'_ [RelationalValue]) -> Result<RelationalExecuteResultSet, CommonError> {
        common_core::logger::trace!("ScyllaCommonSqlConnection - prepare query:{} param:{:?}", query, param);
        
        let prepare = self.get_prepare(query)?;

        let mut result = RelationalExecuteResultSet::default();

        let mut typ = Vec::new();
        let cols_g = prepare.get_result_set_col_specs();
        for col in cols_g.iter() {
            result.cols_name.push(col.name().to_string());
            typ.push(col.typ());
        }

        let real_param = param.iter().fold(Vec::<Option<&dyn SerializeValue>>::new(), |mut acc,x | {
            let p : Option<&dyn SerializeValue> = match x {
                RelationalValue::Int(i) => Some(i),
                RelationalValue::Bin(bs) => Some(bs),
                RelationalValue::Double(f) => Some(f),
                RelationalValue::String(s) => Some(s),
                RelationalValue::Bool(b) => Some(b),
                RelationalValue::Null => None,
                RelationalValue::BigInt(bi) => Some(bi),
                RelationalValue::Float(f) => Some(f),
            };
            acc.push(p);
            acc
        });
        
        let query_result = self.execute_query(&prepare, real_param.as_slice())?;

        if typ.len() <= 0 {
            return Ok(result);
        }

        let rows = self.get_query_row_result(query_result)?;

        let mut row_iter = match rows.rows::<ScyllaFetcherRow>() {
            Ok(ok) => Ok(ok),
            Err(err) => CommonError::new(&CommonDefaultErrorKind::InvalidApiCall,
                                         format!("ScyllaConnection.execute - row_iter - {}", err)).to_result()
        }?;

        while let Some(r) = row_iter.next() {
            let mut convert_row = match r {
                Ok(ok) => Ok(ok),
                Err(err) => CommonError::new(&CommonDefaultErrorKind::InvalidApiCall,
                                             format!("ScyllaConnection.execute - convert_row - {}", err)).to_result()
            }?;

            let chk_err = convert_row.get_error();
            if chk_err.is_err() {
                return CommonError::extend(&CommonDefaultErrorKind::InvalidApiCall, "row read fail", chk_err.err().unwrap()).to_result()
            }
            let col_data = convert_row.clone_col();

            if col_data.len() != result.cols_name.len() {
                return CommonError::new(&CommonDefaultErrorKind::NotMatchArgs,
                                        format!("ScyllaConnection.execute - col_data - data len : {} != col count : {}", col_data.len(), result.cols_name.len())).to_result()
            }

            result.cols_data.push(col_data);
        }

        Ok(result)
    }

    fn get_current_time(&mut self) -> Result<std::time::Duration, CommonError> {
        self.get_current_duration()
    }
}

impl PairExecutor for ScyllaConnection {
    fn execute_pair(&mut self, query: &'_ str, param: &PairValueEnum) -> Result<PairValueEnum, CommonError> {
        let execute_param = if let PairValueEnum::Array(a) = &param {
            let mut p_vec = Vec::new();
            for param_data in a {
                let p_ele : Option<&dyn SerializeValue> = match param_data {
                    PairValueEnum::Int(i) => Some(i),
                    PairValueEnum::Bin(bs) => Some(bs),
                    PairValueEnum::Double(f) => Some(f),
                    PairValueEnum::String(s) => Some(s),
                    PairValueEnum::Bool(b) => Some(b),
                    PairValueEnum::Null => None,
                    PairValueEnum::BigInt(bi) => Some(bi),
                    PairValueEnum::Float(f) => Some(f),
                    _ => {
                        return CommonError::new(&CommonDefaultErrorKind::InvalidApiCall, "not support type")
                            .to_result();
                    }
                };
                p_vec.push(p_ele);
            }
            Ok(p_vec)
        } else {
            CommonError::new(&CommonDefaultErrorKind::InvalidApiCall, "not array type").to_result()
        }?;
        
        
        common_core::logger::trace!("ScyllaCommonSqlConnection - execute_pair query:{} param:{:?}", query, param);
        let prepare = self.get_prepare(query)?;

        let query_result = self.execute_query(&prepare, execute_param.as_slice())?;
        
        let cols_g = prepare.get_result_set_col_specs();

        if cols_g.len() <= 0 {
            return Ok(PairValueEnum::Null);
        }
        
        let mut cache = HashMap::new();
        let mut cache_idx_m = HashMap::new();
        
        {
            let slice = cols_g.as_slice();
            for cols in 0..cols_g.len() {
                cache_idx_m.insert(cols, slice[cols].name());
                cache.entry(slice[cols].name()).or_insert_with(|| {Vec::<PairValueEnum>::new()});
            }    
        }
        
        let rows = self.get_query_row_result(query_result)?;

        let mut row_iter = match rows.rows::<ScyllaPairFetcherRow>() {
            Ok(ok) => Ok(ok),
            Err(err) => CommonError::new(&CommonDefaultErrorKind::InvalidApiCall,
                                         format!("ScyllaConnection.execute_pair - row_iter - {}", err)).to_result()
        }?;

        while let Some(r) = row_iter.next() {
            let mut convert_row = match r {
                Ok(ok) => Ok(ok),
                Err(err) => CommonError::new(&CommonDefaultErrorKind::InvalidApiCall,
                                             format!("ScyllaConnection.execute_pair - convert_row - {}", err)).to_result()
            }?;

            let chk_err = convert_row.get_error();
            
            if chk_err.is_err() {
                return CommonError::extend(&CommonDefaultErrorKind::InvalidApiCall, "row read fail", chk_err.err().unwrap()).to_result()
            }
            let col_data = convert_row.as_slice();
            
            for data_idx in 0..col_data.len() {
                let key = cache_idx_m.get(&data_idx).ok_or_else(|| {
                    CommonError::new(&CommonDefaultErrorKind::NoData, format!("not exists idx : {}", data_idx)) 
                })?;
                
                let v = cache.get_mut(key).ok_or_else(|| {
                    CommonError::new(&CommonDefaultErrorKind::NoData, format!("not exists col idx : {}", data_idx))
                })?;
                
                v.push(col_data[data_idx].clone());
            }
        }

        let mut ret = PairValueEnum::Null;
        let mut convert_m = HashMap::new();
        for item in cache {
            convert_m.insert(item.0.to_string(), PairValueEnum::Array(item.1));
        }
        ret = PairValueEnum::Map(convert_m);
        Ok(ret)
    }

    fn get_current_time(&mut self) -> Result<std::time::Duration, CommonError> {
        self.get_current_duration()
    }
}