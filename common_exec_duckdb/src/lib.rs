mod db_conn;

use common_err::{CommonError};

use common_core::collection::pool::get_thread_safe_pool;
use common_pair_exec::{PairExecutor, PairExecutorInfo, PairExecutorPool};
use db_conn::DuckDBConnection;

pub fn create_duckdb_pair_conn_pool(name : String, info : PairExecutorInfo, alloc_size : usize) -> PairExecutorPool {
    let gen_fn : Box<dyn Fn(()) -> Result<Box<dyn PairExecutor>, CommonError>> = (|info : PairExecutorInfo| {
        let global_info = info;

        let real_fn  = move |_ : ()| {
            let conn = DuckDBConnection::new(global_info.clone().addr.as_str());

            match conn {
                Ok(ok) => Ok(Box::new(ok) as Box<dyn PairExecutor>),
                Err(err) => {Err(err)}
            }
        };

        Box::new(real_fn)
    })(info);

    get_thread_safe_pool(name, gen_fn, alloc_size)
}