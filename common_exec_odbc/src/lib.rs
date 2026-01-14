mod db_conn;

use common_err::CommonError;

use common_core::collection::pool::get_thread_safe_pool;
use common_err::gen::CommonDefaultErrorKind;
use common_pair_exec::{PairExecutor, PairExecutorInfo, PairExecutorPool};
use db_conn::OdbcConnection;

pub fn create_odbc_pair_conn_pool(name : String, info : PairExecutorInfo, alloc_size : usize) -> PairExecutorPool {
    let gen_fn : Box<dyn Fn(()) -> Result<Box<dyn PairExecutor>, CommonError>> = (|info : PairExecutorInfo| {

        let real_fn  = move |_ : ()| {
            let conn_info = info.clone();
            
            if let Some(extend) = conn_info.extend {
                if extend.len() < 3 {
                    return CommonError::new(&CommonDefaultErrorKind::NoData, 
                                            format!("extend count :{}/3", extend.len())).to_result();
                }
                
                let data_source = extend[0].clone();
                let current_time_query = extend[1].clone();
                let current_time_cols_name = extend[2].clone();
                
                let conn = OdbcConnection::new(data_source, 
                                               current_time_query, 
                                               current_time_cols_name);
                match conn {
                    Ok(ok) => Ok(Box::new(ok) as Box<dyn PairExecutor>),
                    Err(err) => {Err(err)}
                }
            } else {
                CommonError::new(&CommonDefaultErrorKind::InvalidApiCall, "extend is null").to_result()
            }
        };

        Box::new(real_fn)
    })(info);

    get_thread_safe_pool(name, gen_fn, alloc_size)
}