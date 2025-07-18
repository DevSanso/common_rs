use common_core::err::*;

pub fn init_err_code_list(category_id :ErrorCategory, errs : Vec<(ErrorCode, ErrorDesc)>) {
    push_error_list(category_id, errs);
}