mod db_conn;
pub(crate) mod types;

use std::error::Error;

use common_core::collection::pool::get_thread_safe_pool;
use common_conn::{CommonSqlConnection, CommonSqlConnectionInfo, CommonSqlConnectionPool};
use db_conn::ScyllaCommonSqlConnection;

pub fn create_scylla_conn_pool(name : String, info : Vec<CommonSqlConnectionInfo>, alloc_size : usize) -> CommonSqlConnectionPool {
    let gen_fn : Box<dyn Fn(()) -> Result<Box<dyn CommonSqlConnection>, Box<dyn Error>>> = (|info : Vec<CommonSqlConnectionInfo>| {
        let global_info = info;

        let real_fn  = move |_ : ()| {
            let conn = ScyllaCommonSqlConnection::new(global_info.clone());
            
            match conn {
                Ok(ok) => Ok(Box::new(ok) as Box<dyn CommonSqlConnection>),
                Err(err) => {Err(err)}
            }
        };

        Box::new(real_fn)
    })(info);

    get_thread_safe_pool(name, gen_fn, alloc_size)
}