mod common_err;

use std::error::Error;
use std::collections::HashMap;
use std::sync::{ RwLockWriteGuard, RwLock, OnceLock};
use std::panic::Location;
use std::sync::atomic::AtomicBool;

use rustc_demangle::demangle;
use backtrace::Backtrace;
use backtrace::BacktraceFrame;
pub use common_err::*;


pub type ErrorCategory = u32;
pub type ErrorCode     = &'static str;

#[derive(Debug, Clone)]

pub struct ErrorDesc(&'static str /* desc*/, #[allow(dead_code)] &'static str /* detail*/);

#[derive(Debug)]
pub struct CommonImplError {
    func : String,
    file : &'static str,
    category : ErrorCategory,
    err_code : ErrorCode,
    desc : ErrorDesc,
    message : String,

    cause_func : String,
    cause_file : &'static str,
    cause_message : String
}

impl ErrorDesc {
    pub fn new(desc : &'static str, detail : &'static str) -> Self {
        ErrorDesc(desc, detail)
    }
}

impl CommonImplError {
    pub fn new(func : String, err_code : ErrorCode, file : &'static str, category : ErrorCategory, desc : ErrorDesc,
     message : String, cause_func : String, cause_file : &'static str, cause_message : String) -> Self {
        CommonImplError {func: func, err_code,  file : file, category : category, desc : desc, message : message, cause_func, cause_file, cause_message }
    }

    pub fn as_error<T>(self) -> Result<T, Box<dyn Error>>{
        Err(Box::new(self))
    }

    pub fn func_name(&self) -> &'_ str {
        self.func.as_str()
    }

    pub fn file_name(&self) -> &'_ str {
        self.file
    }

    pub fn category_id(&self) -> &'_ ErrorCategory {
        &self.category
    }

    pub fn err_desc(&self) -> &'_ ErrorDesc {
        &self.desc
    }

    pub fn msg(&self) -> &'_ str {
        self.message.as_str()
    }

    pub fn cause_func_name(&self) -> &'_ str {
        self.cause_func.as_str()
    }

    pub fn cause_file_name(&self) -> &'_ str {
        self.cause_file
    }

    pub fn cause_msg(&self) -> &'_ str {
        self.cause_message.as_str()
    }

}

impl std::fmt::Display for CommonImplError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let stack_desc= format!("[category:{}][err:{}][file:{}][cause:{}:{}]"
            , self.category, self.err_code, self.file, self.cause_file, self.cause_func);

        let _ = write!(f, "{} - [desc:{},msg:{}] [cause:{}]\n", stack_desc, self.desc.0, self.message, self.cause_message);
        std::fmt::Result::Ok(())
    }
}

impl Error for CommonImplError  {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        None
    }

    fn description(&self) -> &str {
        ""
    }

    fn cause(&self) -> Option<&dyn Error> {
        self.source()
    }
}
struct ErrorList {
    init_onces : HashMap<ErrorCategory, AtomicBool>,
    errors : HashMap<(ErrorCategory, ErrorCode), ErrorDesc>
}

static GLOBAL_ERROR_LIST : OnceLock<RwLock<ErrorList>> = OnceLock::new();
static GLOBAL_COMMON_ERR_LIST_INIT : AtomicBool = AtomicBool::new(false);

unsafe fn call_once_common_init() {
    if GLOBAL_COMMON_ERR_LIST_INIT.load(std::sync::atomic::Ordering::SeqCst) == true {
        return;
    }

    let _ = GLOBAL_ERROR_LIST.get_or_init(|| {
        RwLock::new(ErrorList { init_onces: HashMap::new(), errors: HashMap::new() })
    });

    {
        let g = GLOBAL_ERROR_LIST.get().unwrap().write().unwrap();
        common_error_push(g);
    }

    GLOBAL_COMMON_ERR_LIST_INIT.store(true, std::sync::atomic::Ordering::SeqCst);
}

unsafe fn common_error_push(mut g : RwLockWriteGuard<'_, ErrorList>) {
    {
        let is_once = g.init_onces.entry(COMMON_ERROR_CATEGORY).or_insert(AtomicBool::new(false));

        if is_once.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return;
        }
    }

    let mut errs = common_err::_gen_err_list();
    while let Some(err_data) = errs.pop() {
        g.errors.insert((COMMON_ERROR_CATEGORY, err_data.0), ErrorDesc::new(err_data.1.0, err_data.1.1));
    }
}

pub fn push_error_list(category_id :ErrorCategory, mut errs : Vec<(ErrorCode, ErrorDesc)>) {
    unsafe {
        call_once_common_init();
    
        {
            let mut g = GLOBAL_ERROR_LIST.get().unwrap().write().unwrap();

            let is_once = g.init_onces.entry(category_id).or_insert(AtomicBool::new(false));
    
            if is_once.swap(true, std::sync::atomic::Ordering::SeqCst) {
                return;
            }

            while let Some(err_data) = errs.pop() {
                g.errors.insert((category_id, err_data.0), err_data.1);
            }
        }
    }
    
}

pub fn push_error_list_str_tup(category_id :ErrorCategory, mut errs : Vec<(&'static str, (&'static str, &'static str))>) {
    unsafe {
        call_once_common_init();
    
        {
            let mut g = GLOBAL_ERROR_LIST.get().unwrap().write().unwrap();

            let is_once = g.init_onces.entry(category_id).or_insert(AtomicBool::new(false));
    
            if is_once.swap(true, std::sync::atomic::Ordering::SeqCst) {
                return;
            }

            while let Some(err_data) = errs.pop() {
                g.errors.insert((COMMON_ERROR_CATEGORY, err_data.0), ErrorDesc::new(err_data.1.0, err_data.1.1));
            }
        }
    }
    
}

fn decode_bt_frames(frames : &[BacktraceFrame]) -> String {
    if let Some(frame) = frames.get(1) {
        for symbol in frame.symbols() {
            if let Some(name) = symbol.name() {
                let de = demangle(name.as_str().unwrap()).to_string();
                
                
                let parts: Vec<&str> = de.as_str().split("::").collect();

                if parts.len() >= 2 && parts.last().unwrap().starts_with('h') {
                    return parts.get(parts.len() - 2).unwrap().to_string();
                }
            
                return parts.last().unwrap().to_string();
            }
        }
    }

    return String::from("")
}

#[track_caller]
pub fn create_error(category_id :ErrorCategory, code : ErrorCode, msg : String, source : Option<Box<dyn Error>>) -> CommonImplError {
    unsafe {
        call_once_common_init();
    }
    
    let loc = Location::caller();

    let mut bt = Backtrace::new_unresolved();
    bt.resolve();


    let func = decode_bt_frames(bt.frames());
    let file = loc.file();

    let mut cause_fn = String::from("");
    let mut cause_file : &'static str = "";
    let mut cause_message = String::from("");

    if source.is_none() {
        cause_fn.insert_str(0, func.as_str());
        cause_file = file;
        cause_message.insert_str(0, msg.as_str());

    } else {
        let src = source.unwrap();

        let temp_msg = format!("[msg:{}, src:{}]", msg, src.to_string());
        let (fn_ref, file_ref, msg_ref) = match src.downcast_ref::<CommonImplError>() {
            Some(e) => (e.cause_func.as_str(), e.cause_file, e.cause_message.as_str()),
            None => (func.as_str(), file, temp_msg.as_str())
        };
        cause_fn.insert_str(0, fn_ref);
        cause_file = file_ref;
        cause_message.insert_str(0, msg_ref);
    }

    CommonImplError::new(func, code, file, category_id, {
        let g = GLOBAL_ERROR_LIST.get().unwrap().read().unwrap();
        let e = match g.errors.get(&(category_id, code)) {
            Some(s) => s,
            None => g.errors.get(&(COMMON_ERROR_CATEGORY, UNKNOWN_ERROR)).unwrap()
        };
        e.clone()
    }, msg, cause_fn, cause_file, cause_message)
}
