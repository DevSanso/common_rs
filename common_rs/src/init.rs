mod signal;
mod logger;

pub struct InitConfig<'a> {
    pub log_level : &'a str,
    pub log_file : Option<&'a str>
}
pub fn init_common(cfg : InitConfig<'_>) -> Result<(), Box<dyn std::error::Error>>{
    signal::init_once();
    logger::init_once(cfg.log_level, cfg.log_file)?;
    Ok(())
}