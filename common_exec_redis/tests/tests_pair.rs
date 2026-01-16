use std::collections::HashMap;
use toml;
use common_core::utils::func::generate_random_string;
use common_err::CommonError;
use common_err::gen::CommonDefaultErrorKind;
use common_exec_redis::create_redis_pair_conn_pool;
use common_pair_exec::{PairExecutor, PairExecutorInfo, PairValueEnum};

fn connect_redis_db() -> Result<common_pair_exec::PairExecutorPool, CommonError> {
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

    let p = create_redis_pair_conn_pool("test".to_string(), info, 5);
    Ok(p)
}
#[test]
fn test_connect_get_now() -> Result<(), CommonError> {
    let p = connect_redis_db()?;
    let mut item = p.get_owned(())?;
    let conn = item.get_value();
    let current = conn.get_current_time()?;

    println!("##SELECT_CURRENT: {:?}", current);

    Ok(())
}

#[test]
fn test_connect_insert() -> Result<(), CommonError> {
    let p = connect_redis_db()?;
    let mut item = p.get_owned(())?;
    let conn = item.get_value();

    for i in 1..5000 {
        let value = generate_random_string(100);
        conn.execute_pair("set", &PairValueEnum::Array(vec![
            PairValueEnum::BigInt(i as i64), PairValueEnum::String(value)
        ]))?;

        conn.execute_pair("del", &PairValueEnum::Array(vec![
            PairValueEnum::BigInt(i as i64)
        ]))?;
    }

    Ok(())
}

#[test]
fn test_connect_select_large() -> Result<(), CommonError> {
    let p = connect_redis_db()?;
    let mut item = p.get_owned(())?;
    let conn = item.get_value();

    for i in 0..5000 {
        conn.execute_pair("del", &PairValueEnum::Array(vec![
            PairValueEnum::BigInt(i as i64)
        ]))?;
    }

    for i in 0..5000 {
        let value = generate_random_string(100);
        conn.execute_pair("set", &PairValueEnum::Array(vec![
            PairValueEnum::BigInt(i as i64), PairValueEnum::String(value)
        ]))?;


    }
    let timer = std::time::SystemTime::now();
    let array_ret = conn.execute_pair("mget", &PairValueEnum::Array(
        (0..5000).fold(vec![], |mut acc, i| { acc.push(PairValueEnum::BigInt(i));acc})
    ))?;
    let elap = timer.elapsed().unwrap();
    println!("##ELAPSED: {:?}", elap);

    if let PairValueEnum::Map(m) = array_ret {
        if let Some(PairValueEnum::Array(a)) = m.get("0") {
            assert_eq!(a.len(), 5000);
        }
        else {
            assert!(false);
        }
    } else {
        assert!(false);
    }

    for i in 0..5000 {
        conn.execute_pair("del", &PairValueEnum::Array(vec![
            PairValueEnum::BigInt(i as i64)
        ]))?;
    }

    Ok(())
}

#[test]
fn test_connect_select_list() -> Result<(), CommonError> {
    let p = connect_redis_db()?;
    let mut item = p.get_owned(())?;
    let conn = item.get_value();

    for i in 0..5000 {

    }

    let test_key = PairValueEnum::String("list_testing".to_string());

    conn.execute_pair("del", &PairValueEnum::Array(vec![
        test_key.clone()
    ]))?;

    for i in 0..5000 {
        let value = generate_random_string(100);
        conn.execute_pair("lpush", &PairValueEnum::Array(vec![
            test_key.clone(), PairValueEnum::String(value)
        ]))?;


    }
    let timer = std::time::SystemTime::now();
    let array_ret = conn.execute_pair("lrange", &PairValueEnum::Array(
        vec![test_key.clone(), PairValueEnum::BigInt(0), PairValueEnum::BigInt(5000)]
    ))?;

    let elap = timer.elapsed().unwrap();
    println!("##ELAPSED: {:?}", elap);

    if let PairValueEnum::Map(m) = array_ret {
        if let Some(PairValueEnum::Array(a)) = m.get("0") {
            assert_eq!(a.len(), 5000);
        }
        else {
            assert!(false);
        }
    } else {
        assert!(false);
    }

    conn.execute_pair("del", &PairValueEnum::Array(vec![
        test_key.clone()
    ]))?;

    Ok(())
}