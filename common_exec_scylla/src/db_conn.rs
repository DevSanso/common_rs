mod util;

use scylla::serialize::value::SerializeValue;
use tokio::runtime::{Builder, Runtime};
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use common_err::{CommonError, gen::CommonDefaultErrorKind};
use common_relational_exec::{RelationalExecutorInfo, RelationalExecuteResultSet, RelationalExecutor, RelationalValue};
use util::ScyllaFetcherRow;

pub struct ScyllaConnection {
    session : Session,
    rt : Runtime
}

impl ScyllaConnection {
    pub(crate) fn new(infos : Vec<RelationalExecutorInfo>) -> Result<Self, CommonError> {
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
}
impl RelationalExecutor<RelationalValue> for ScyllaConnection {
    fn execute(&mut self, query : &'_ str, param : &'_ [RelationalValue]) -> Result<RelationalExecuteResultSet, CommonError> {
        common_core::logger::trace!("ScyllaCommonSqlConnection - prepare query:{} param:{:?}", query, param);

        let feature = self.session.prepare(query);

        let prepare = match self.rt.block_on(feature) {
            Ok(ok) => Ok(ok),
            Err(err) => CommonError::new(&CommonDefaultErrorKind::InvalidApiCall,
                                         format!("ScyllaConnection.execute - {}", err)).to_result()
        }?;

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

        let feature = self.session.execute_unpaged(&prepare, real_param);
        let query_result = match self.rt.block_on(feature) {
            Ok(ok) => Ok(ok),
            Err(err) => CommonError::new(&CommonDefaultErrorKind::ExecuteFail,
                                         format!("ScyllaConnection.execute - block_on - {}", err)).to_result()
        }?;

        if typ.len() <= 0 {
            return Ok(result);
        }

        let rows = match query_result.into_rows_result() {
            Ok(ok) => Ok(ok),
            Err(err) => CommonError::new(&CommonDefaultErrorKind::InvalidApiCall,
                                         format!("ScyllaConnection.execute - query_result - {}", err)).to_result()
        }?;

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
        let ret = self.execute("SELECT CAST(toUnixTimestamp(now()) AS BIGINT) AS unix_timestamp  FROM system.local", &[])?;

        if ret.cols_data.len() <= 0 && ret.cols_data[0].len() <= 0 {
            return CommonError::new(&CommonDefaultErrorKind::NotMatchArgs,  "ScyllaConnection.get_current_time").to_result();
        }

        let data = match ret.cols_data[0][0] {
            RelationalValue::BigInt(bi) => bi,
            RelationalValue::Int(i) => i as i64,
            _ => 0
        };

        Ok(std::time::Duration::from_secs(data as u64))
    }
}