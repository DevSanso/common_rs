use common_core::err::{ErrorCategory, ErrorCode, ErrorDesc, push_error_list};


pub const COMMON_CONN_ERROR_CATEGORY : ErrorCategory = 2;

pub fn common_conn_init() {
    push_error_list(COMMON_CONN_ERROR_CATEGORY, vec![
        ("GetConnectionFailedError", ErrorDesc::new("get other process connection", "check other process state")),
        ("CommandRunError", ErrorDesc::new("running command or query is error", "check query or command")),
        ("ConnectionApiCallError", ErrorDesc::new("connection api function return error", "check server env or process state")),
        ("ResponseScanError", ErrorDesc::new("connection response data read error", "check server state or error handling code")),
    ]);
}