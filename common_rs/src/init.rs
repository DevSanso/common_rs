use common_err::CommonError;
use common_err::gen::CommonDefaultErrorKind;

pub(crate) mod signal;
mod logger;

pub struct InitConfig<'a> {
    pub log_level : &'a str,
    pub log_file : Option<&'a str>,
    pub log_file_size_mb : u64,
}
pub fn init_common(cfg : InitConfig<'_>) -> Result<(), CommonError>{
    signal::init_once();
    logger::init_once(cfg.log_level, cfg.log_file, cfg.log_file_size_mb).map_err(|e| {
        CommonError::extend(&CommonDefaultErrorKind::InitFailed, "can't success init_once", e)
    })?;
    Ok(())
}