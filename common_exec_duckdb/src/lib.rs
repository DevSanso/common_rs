mod db_conn;

use std::error::Error;

use common_core::collection::pool::get_thread_safe_pool;
use common_relational_exec::{RelationalExecutorInfo, RelationalExecutorPool, RelationalExecutor, RelationalValue};
use db_conn::DuckDBConnection;

pub fn create_scylla_conn_pool(name : String, info : RelationalExecutorInfo, alloc_size : usize) -> RelationalExecutorPool<RelationalValue> {
    let gen_fn : Box<dyn Fn(()) -> Result<Box<dyn RelationalExecutor<RelationalValue>>, Box<dyn Error>>> = (|info : RelationalExecutorInfo| {
        let global_info = info;

        let real_fn  = move |_ : ()| {
            let conn = DuckDBConnection::new(global_info.clone());

            match conn {
                Ok(ok) => Ok(Box::new(ok) as Box<dyn RelationalExecutor<RelationalValue>>),
                Err(err) => {Err(err)}
            }
        };

        Box::new(real_fn)
    })(info);

    get_thread_safe_pool(name, gen_fn, alloc_size)
}