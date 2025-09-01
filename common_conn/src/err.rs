use common_core::err::{push_error_list_str_tup, ErrorCategory};
use common_core::utils::macros::gen_err_msg_list;


pub const COMMON_CONN_ERROR_CATEGORY : ErrorCategory = 2;

pub fn common_conn_init() {
    push_error_list_str_tup(COMMON_CONN_ERROR_CATEGORY, _gen_err_list());
}

gen_err_msg_list![
    (GET_CONNECTION_FAILED_ERROR, "get other process connection", "check other process state"),
    (COMMAND_RUN_ERROR, "running command or query is error", "check query or command"),
    (CONNECTION_API_CALL_ERROR, "connection api function return error", "check server env or process state"),
    (RESPONSE_SCAN_ERROR, "connection response data read error", "check server state or error handling code"),
    (TRANSACTION_CALL_ERROR, "connection transcation call error", "check server state or error handling code")
];