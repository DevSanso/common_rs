use std::collections::HashMap;
use toml;
use common_core::utils::func::generate_random_string;
use common_err::CommonError;
use common_err::gen::CommonDefaultErrorKind;
use common_exec_pg::create_pg_pair_conn_pool;
use common_pair_exec::{PairExecutor, PairExecutorInfo, PairValueEnum};

fn connect_pg_db() -> Result<common_pair_exec::PairExecutorPool, CommonError> {
    let read_toml : HashMap<String, String> = toml::from_str(include_str!("./tests.asset.toml")).map_err(|e| {
        CommonError::new(&CommonDefaultErrorKind::Etc, e.to_string())
    })?;
    let info = PairExecutorInfo {
        addr: read_toml["addr"].clone(),
        name: read_toml["name"].clone(),
        user: read_toml["user"].clone(),
        password: read_toml["password"].clone(),
        timeout_sec: 3600,
        extend: None
    };

    let p = create_pg_pair_conn_pool("test".to_string(), info, 5);
    Ok(p)
}

fn create_large_table(conn : &mut Box<dyn PairExecutor>) -> Result<(), CommonError> {

    const TABLE : &'static str = "create table if not exists large_data(
    id bigint, name text, hash varchar(32), data text,
    primary key (id,hash)
    )";

    conn.execute_pair(TABLE, &PairValueEnum::Null)?;

    Ok(())
}

fn insert_large_data(conn : &mut Box<dyn PairExecutor>, count : usize) -> Result<(), CommonError> {
    for i in 0..count {
        conn.execute_pair("insert into large_data(id, name, hash, data) values($1,$2,$3,$4)",
        &PairValueEnum::Array(vec![
            PairValueEnum::BigInt(i as i64), PairValueEnum::String("hello".to_string()),
            PairValueEnum::String(generate_random_string(32)),
            PairValueEnum::String(generate_random_string(100))
        ]))?;
    }
    Ok(())
}

fn drop_large_table(conn : &mut Box<dyn PairExecutor>) -> Result<(), CommonError> {
    const TABLE : &'static str = "drop table if exists large_data";

    conn.execute_pair(TABLE, &PairValueEnum::Null)?;
    Ok(())
}

#[test]
fn test_connect_get_now() -> Result<(), CommonError> {
    let p = connect_pg_db()?;
    let mut item = p.get_owned(())?;
    let conn = item.get_value();
    let timer = std::time::SystemTime::now();

    let current = conn.get_current_time()?;
    let elap = timer.elapsed().unwrap();
    println!("##elap time : {:?}", elap.as_millis());
    println!("##SELECT_CURRENT: {:?}", current);

    Ok(())
}

#[test]
fn test_connect_insert() -> Result<(), CommonError> {
    let p = connect_pg_db()?;
    let mut item = p.get_owned(())?;
    let conn = item.get_value();

    create_large_table(conn)?;

    let timer = std::time::SystemTime::now();
    let ret = insert_large_data(conn, 5000);
    if ret.is_err() {
        drop_large_table(conn)?;

        return ret.err().unwrap().to_result();
    }

    let elap = timer.elapsed().unwrap();
    println!("##elap time : {:?}", elap.as_millis());

    drop_large_table(conn)?;

    Ok(())
}

#[test]
fn test_connect_select_large() -> Result<(), CommonError> {
    let p = connect_pg_db()?;
    let mut item = p.get_owned(())?;
    let conn = item.get_value();

    create_large_table(conn)?;

    let ret = insert_large_data(conn, 10000);
    if ret.is_err() {
        drop_large_table(conn)?;

        return ret.err().unwrap().to_result();
    }
    let timer = std::time::SystemTime::now();
    let ret = conn.execute_pair("select * from large_data", &PairValueEnum::Null);
    let elap = timer.elapsed().unwrap();
    println!("##elap time : {:?}", elap.as_millis());

    if ret.is_err() {
        drop_large_table(conn)?;
        return ret.err().unwrap().to_result();
    }

    if let PairValueEnum::Map(ret) = ret.unwrap() {
        let PairValueEnum::Array(id) =  ret.get("id").unwrap()else {
            panic!("id")
        };

        let PairValueEnum::Array(name) =  ret.get("name").unwrap() else {
            panic!("name")
        };

        let PairValueEnum::Array(hash) =  ret.get("hash").unwrap()else {
            panic!("hash")
        };

        let PairValueEnum::Array(data) =  ret.get("data").unwrap() else {
            panic!("data")
        };

        println!("##SELECT_LARGE_TABLE_CNT: {} {} {} {}", id.len(), name.len(), hash.len(), data.len());
    }

    drop_large_table(conn)?;

    Ok(())
}