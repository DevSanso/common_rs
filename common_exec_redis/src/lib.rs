mod db_conn;

use common_err::CommonError;

use common_core::collection::pool::get_thread_safe_pool;
use common_pair_exec::{PairExecutor, PairExecutorInfo, PairExecutorPool};
use db_conn::RedisConnection;


pub fn create_redis_pair_conn_pool(name : String, info : PairExecutorInfo, alloc_size : usize) -> PairExecutorPool {
    let gen_fn : Box<dyn Fn(()) -> Result<Box<dyn PairExecutor>, CommonError>> = (|info : PairExecutorInfo| {

        let real_fn  = move |_ : ()| {
            let conn_info = info.clone();
            let conn = RedisConnection::new(
                conn_info.addr.as_str(), 
                conn_info.user.as_str(), 
                conn_info.password.as_str(), 
                conn_info.name.as_str());

            match conn {
                Ok(ok) => Ok(Box::new(ok) as Box<dyn PairExecutor>),
                Err(err) => {Err(err)}
            }
        };

        Box::new(real_fn)
    })(info);

    get_thread_safe_pool(name, gen_fn, alloc_size)
}