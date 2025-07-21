use super::*;

use crate::utils::macros::gen_err_msg_list;

pub const COMMON_ERROR_CATEGORY : ErrorCategory = 1;

gen_err_msg_list![
    (MAX_SIZED_ERROR, "memeory or pool is used Max size", "can't alloc new memory"),
    (NOT_MATCH_ARGS_LEN_ERROR, "query bound args count not mathcing", "query parameter length not maching, check bound varibles"),
    (PARSING_ERROR, "failed parsing data", "check data"),
    (NO_DATA_ERROR, "not exists data error", "check data"),
    (OVERFLOW_SIZE_ERROR, "overflow size error", "check array size or range size"),
    (API_CALL_ERROR, "system api call function is return error", "check server code"),
    (NO_SUPPORT_ERROR, "not support is function", "check server code and data"),
    (CRITICAL_ERROR, "critical error", "system critial error, check os status and code"),
    (UNKNOWN_ERROR, "unknown error", "check system"),
    (FILE_IO_ERROR, "file io error", "check system env")
];