mod shell_conn;

use common_core::collection::pool::get_thread_safe_pool;
use common_err::CommonError;
use common_relational_exec::{RelationalExecutorPool, RelationalExecutor, RelationalValue};
use shell_conn::LocalShellConnection;

#[derive(Clone)]
pub struct ShellParam {
    pub sep : String,
    pub next : String
}

pub fn create_shell_conn_pool(name : String, alloc_size : usize) -> RelationalExecutorPool<RelationalValue> {
    let gen_fn : Box<dyn Fn(()) -> Result<Box<dyn RelationalExecutor<RelationalValue>>, CommonError>> = (|| {
  
        let real_fn   = move |_ : ()| {
            let conn = LocalShellConnection::new();
            Ok(Box::new(conn) as Box<dyn RelationalExecutor<RelationalValue>>)
        };

        Box::new(real_fn)
    })();

    get_thread_safe_pool(name, gen_fn, alloc_size)
}