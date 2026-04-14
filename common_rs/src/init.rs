use common_err::CommonError;
use common_err::gen::CommonDefaultErrorKind;
use crate::init::logger::LoggerConf;

pub(crate) mod signal;
pub(crate) mod logger;

pub struct InitConfig {
    pub logger_conf : LoggerConf,
}
pub fn init_common(cfg : InitConfig) -> Result<(), CommonError>{
    signal::init_once();
    logger::init_once(cfg.logger_conf).map_err(|e| {
        CommonError::extend(&CommonDefaultErrorKind::InitFailed, "can't success init_once", e)
    })?;
    Ok(())
}