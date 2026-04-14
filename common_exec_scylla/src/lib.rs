mod db_conn;

use common_err::CommonError;

use common_core::collection::pool::get_thread_safe_pool;
use common_pair_exec::{PairExecutor, PairExecutorInfo, PairExecutorPool};
use db_conn::ScyllaConnection;
use crate::db_conn::ScyllaConnInfo;

pub fn create_scylla_pair_conn_pool(name : String, info : PairExecutorInfo, alloc_size : usize) -> PairExecutorPool {
    let gen_fn : Box<dyn Fn(()) -> Result<Box<dyn PairExecutor>, CommonError>> = (|info : PairExecutorInfo| {
        let real_fn  = move |_ : ()| {
            let conn_info = ScyllaConnInfo {
                addr: info.addr.clone(),
                name: info.name.clone(),
                user: info.user.clone(),
                password: info.password.clone(),
                timeout_sec: info.timeout_sec,
            };
            let conn = ScyllaConnection::new(conn_info);

            match conn {
                Ok(ok) => Ok(Box::new(ok) as Box<dyn PairExecutor>),
                Err(err) => {Err(err)}
            }
        };

        Box::new(real_fn)
    })(info);

    get_thread_safe_pool(name, gen_fn, alloc_size)
}