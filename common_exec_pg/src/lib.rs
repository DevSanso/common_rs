mod db_conn;

use common_err::CommonError;

use common_core::collection::pool::get_thread_safe_pool;
use common_pair_exec::{PairExecutor, PairExecutorInfo, PairExecutorPool};
use common_relational_exec::{RelationalExecutorInfo, RelationalExecutorPool, RelationalExecutor, RelationalValue};
use db_conn::PostgresConnection;

pub fn create_pg_conn_pool(name : String, info : RelationalExecutorInfo, alloc_size : usize) -> RelationalExecutorPool<RelationalValue> {
    let gen_fn : Box<dyn Fn(()) -> Result<Box<dyn RelationalExecutor<RelationalValue>>, CommonError>> = (|info : RelationalExecutorInfo| {
        
        let real_fn  = move |_ : ()| {
            let conn_info = info.clone();
            let conn = PostgresConnection::new(conn_info.user.as_str(), 
                                               conn_info.password.as_str(),conn_info.addr.as_str(), conn_info.name.as_str());

            match conn {
                Ok(ok) => Ok(Box::new(ok) as Box<dyn RelationalExecutor<RelationalValue>>),
                Err(err) => {Err(err)}
            }
        };

        Box::new(real_fn)
    })(info);

    get_thread_safe_pool(name, gen_fn, alloc_size)
}

pub fn create_pg_pair_conn_pool(name : String, info : PairExecutorInfo, alloc_size : usize) -> PairExecutorPool {
    let gen_fn : Box<dyn Fn(()) -> Result<Box<dyn PairExecutor>, CommonError>> = (|info : PairExecutorInfo| {

        let real_fn  = move |_ : ()| {
            let conn_info = info.clone();
            let conn = PostgresConnection::new(conn_info.user.as_str(),
                                               conn_info.password.as_str(),conn_info.addr.as_str(), conn_info.name.as_str());

            match conn {
                Ok(ok) => Ok(Box::new(ok) as Box<dyn PairExecutor>),
                Err(err) => {Err(err)}
            }
        };

        Box::new(real_fn)
    })(info);

    get_thread_safe_pool(name, gen_fn, alloc_size)
}