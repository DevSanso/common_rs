use super::*;

pub const COMMON_ERROR_CATEGORY : ErrorCategory = 1;

pub(super) fn get_common_error_list() -> Vec<(ErrorCode, ErrorDesc)> {
    vec![
        ("MaxSizedError", ErrorDesc::new("memeory or pool is used Max size", "can't alloc new memory")),
        ("NotMatchArgsLenError", ErrorDesc::new("query bound args count not mathcing", "query parameter length not maching, check bound varibles")),
        ("ParsingError", ErrorDesc::new("failed parsing data", "check data")),
        ("NoDataError", ErrorDesc::new("not exists data error", "check data")),
        ("OverflowSizeError", ErrorDesc::new("overflow size error", "check array size or range size")),
        ("ApiCallError", ErrorDesc::new("system api call function is return error", "check server code")),
        ("NoSupportError", ErrorDesc::new("not support is function", "check server code and data")),
        ("CriticalError", ErrorDesc::new("critical error", "system critial error, check os status and code")),
        ("UnknownError", ErrorDesc::new("unknown error", "check system")),
        ("FileIoError", ErrorDesc::new( "file io error", "check system env"))
    ]
}

