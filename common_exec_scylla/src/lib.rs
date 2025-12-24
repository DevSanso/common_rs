mod db_conn;

use common_err::CommonError;

use common_core::collection::pool::get_thread_safe_pool;
use common_pair_exec::{PairExecutor, PairExecutorInfo, PairExecutorPool};
use db_conn::ScyllaConnection;
use crate::db_conn::ScyllaConnInfo;

pub fn create_scylla_pair_conn_pool(name : String, info : Vec<PairExecutorInfo>, alloc_size : usize) -> PairExecutorPool {
    let gen_fn : Box<dyn Fn(()) -> Result<Box<dyn PairExecutor>, CommonError>> = (|info : Vec<PairExecutorInfo>| {
        let real_fn  = move |_ : ()| {
            let conn_info = info.clone().iter().fold(vec![], |mut acc, x| {
                acc.push(ScyllaConnInfo {
                    addr: x.addr.clone(),
                    name: x.name.clone(),
                    user: x.user.clone(),
                    password: x.password.clone(),
                    timeout_sec: x.timeout_sec,
                });

                acc
            });
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