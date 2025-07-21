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
    desc : ErrorDesc,
    message : String
}

impl ErrorDesc {
    pub fn new(desc : &'static str, detail : &'static str) -> Self {
        ErrorDesc(desc, detail)
    }
}

impl CommonImplError {
    pub fn new(func : String, file : &'static str, category : ErrorCategory, desc : ErrorDesc, message : String) -> Self {
        CommonImplError {func: func, file : file, category : category, desc : desc, message : message}
    }

    pub fn as_error<T>(self) -> Result<T, Box<dyn Error>>{
        Err(Box::new(self))
    }
}

impl std::fmt::Display for CommonImplError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[category:{}][func:{}][file:{}][desc:{}] - {}\n",
            self.category, self.func.as_str(), self.file, self.desc.0, self.message)?;
        
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
    init_onces : HashMap<ErrorCategory, AtomicBool>,
    errors : HashMap<(ErrorCategory, ErrorCode), ErrorDesc>
}

static GLOBAL_ERROR_LIST : OnceLock<RwLock<ErrorList>> = OnceLock::new();

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
        let _ = GLOBAL_ERROR_LIST.get_or_init(|| {
            RwLock::new(ErrorList { init_onces: HashMap::new(), errors: HashMap::new() })
        });
    
        {
            let g = GLOBAL_ERROR_LIST.get().unwrap().write().unwrap();
            common_error_push(g);
        }
    
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
        let _ = GLOBAL_ERROR_LIST.get_or_init(|| {
            RwLock::new(ErrorList { init_onces: HashMap::new(), errors: HashMap::new() })
        });
    
        {
            let g = GLOBAL_ERROR_LIST.get().unwrap().write().unwrap();
            common_error_push(g);
        }
    
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
                let de = demangle(name.as_str().unwrap());
                return format!("{}", de);
            }
        }
    }

    return String::from("")
}

#[track_caller]
pub fn create_error(category_id :ErrorCategory, code : ErrorCode, msg : String) -> CommonImplError {
    let loc = Location::caller();

    let mut bt = Backtrace::new_unresolved();
    bt.resolve();

    let func = decode_bt_frames(bt.frames());

    CommonImplError::new(func, loc.file(), category_id, {
        let g = GLOBAL_ERROR_LIST.get().unwrap().read().unwrap();
        let e = match g.errors.get(&(category_id, code)) {
            Some(s) => s,
            None => g.errors.get(&(COMMON_ERROR_CATEGORY, UNKNOWN_ERROR)).unwrap()
        };
        e.clone()
    }, msg)
}
