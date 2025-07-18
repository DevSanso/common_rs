mod common_err;

use std::error::Error;
use std::collections::HashMap;
use std::sync::Once;
use std::sync::OnceLock;

pub use common_err::COMMON_ERROR_CATEGORY;

pub type ErrorCategory = u32;
pub type ErrorCode     = &'static str;

#[derive(Debug)]
pub struct ErrorDesc(&'static str /* desc*/, &'static str /* detail*/);

#[derive(Debug)]
pub struct CommonImplError {
    func : &'static str,
    file : &'static str,
    category : ErrorCategory,
    desc : &'static ErrorDesc,
    message : String
}

impl ErrorDesc {
    pub fn new(desc : &'static str, detail : &'static str) -> Self {
        ErrorDesc(desc, detail)
    }
}

impl CommonImplError {
    pub fn new(func : &'static str, file : &'static str, category : ErrorCategory, desc : &'static ErrorDesc, message : String) -> Self {
        CommonImplError {func: func, file : file, category : category, desc : desc, message : message}
    }
}

impl std::fmt::Display for CommonImplError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[category:{}][func:{}][file:{}][desc:{}] - {}\n",
            self.category, self.func, self.file, self.desc.0, self.message)?;
        
        std::fmt::Result::Ok(())
    }
}

impl Error for CommonImplError  {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        None
    }

    fn description(&self) -> &str {
        stringify!(CommonError)
    }

    fn cause(&self) -> Option<&dyn Error> {
        self.source()
    }
}
struct ErrorList {
    init_onces : HashMap<ErrorCategory, Once>,
    errors : HashMap<(ErrorCategory, ErrorCode), ErrorDesc>
}

static mut GLOBAL_ERROR_LIST : OnceLock<ErrorList> = OnceLock::new();

unsafe fn common_error_push(g : &mut ErrorList) {
    let o = g.init_onces.entry(COMMON_ERROR_CATEGORY).or_insert(Once::new());
    let mut errs = common_err::get_common_error_list();
    o.call_once(|| {
        while let Some(err_data) = errs.pop() {
            g.errors.insert((COMMON_ERROR_CATEGORY, err_data.0), err_data.1);
        }
    });
}

pub fn push_error_list(category_id :ErrorCategory, mut errs : Vec<(ErrorCode, ErrorDesc)>) {
    unsafe {
        let _ = GLOBAL_ERROR_LIST.get_or_init(|| {
            ErrorList { init_onces: HashMap::new(), errors: HashMap::new() }
        });
    
        let g = GLOBAL_ERROR_LIST.get_mut().unwrap();
        common_error_push(g);
    
        let o = g.init_onces.entry(category_id).or_insert(Once::new());
        o.call_once(|| {
            while let Some(err_data) = errs.pop() {
                g.errors.insert((category_id, err_data.0), err_data.1);
            }
        });
    }
    
}
pub fn create_error(func : &'static str, file : &'static str,
    category_id :ErrorCategory, code : ErrorCode, msg : String) -> CommonImplError {
    unsafe {
        let g = GLOBAL_ERROR_LIST.get().unwrap();
        let e = match g.errors.get(&(category_id, code)) {
            Some(s) => s,
            None => g.errors.get(&(COMMON_ERROR_CATEGORY, "UnknownError")).unwrap()
        };

        CommonImplError::new(func, file, category_id, e, msg)
    }
}