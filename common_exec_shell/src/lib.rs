mod shell_conn;

use std::error::Error;

use common_core::collection::pool::get_thread_safe_pool;
use common_relational_exec::{RelationalExecutorPool, RelationalExecutor};
use shell_conn::LocalShellConnection;

pub struct ShellParam {
    pub sep : String,
    pub next : String,
    pub args : Vec<String>,
}

pub fn create_shell_conn_pool(name : String, alloc_size : usize) -> RelationalExecutorPool<ShellParam> {
    let gen_fn : Box<dyn Fn(()) -> Result<Box<dyn RelationalExecutor<ShellParam>>, Box<dyn Error>>> = (|| {

        let real_fn  = move |_ : ()| {
            let conn = LocalShellConnection::new();
            Ok(Box::new(conn) as Box<dyn RelationalExecutor<ShellParam>>)
        };

        Box::new(real_fn)
    })();

    get_thread_safe_pool(name, gen_fn, alloc_size)
}