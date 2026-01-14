use std::collections::HashMap;
use common_err::CommonError;
use common_err::gen::CommonDefaultErrorKind;
use common_exec_odbc::create_odbc_pair_conn_pool;
use common_pair_exec::PairExecutorInfo;


fn connect_odbc_db() -> Result<common_pair_exec::PairExecutorPool, CommonError> {
    let read_toml : HashMap<String, Vec<String>> = toml::from_str(include_str!("./tests.asset.toml")).map_err(|e| {
        CommonError::new(&CommonDefaultErrorKind::Etc, e.to_string())
    })?;
    let info = PairExecutorInfo {
        addr: String::from(""),
        name: String::from(""),
        user: String::from(""),
        password: String::from(""),
        timeout_sec: 3600,
        extend: Some(read_toml["extend"].clone())
    };

    let p = create_odbc_pair_conn_pool("test".to_string(), info, 5);
    Ok(p)
}
#[test]
fn test_connect_and_get_now() -> Result<(), CommonError>  {
    let p = connect_odbc_db()?;
    let mut item = p.get_owned(())?;
    let conn = item.get_value();
    let timer = std::time::SystemTime::now();

    let current = conn.get_current_time()?;
    let elap = timer.elapsed().unwrap();
    println!("##elap time : {:?}", elap.as_millis());
    println!("##SELECT_CURRENT: {:?}", current);
    Ok(())

}