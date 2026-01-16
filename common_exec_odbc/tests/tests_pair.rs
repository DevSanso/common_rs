use std::collections::HashMap;
use common_err::CommonError;
use common_err::gen::CommonDefaultErrorKind;
use common_exec_odbc::create_odbc_pair_conn_pool;
use common_pair_exec::{PairExecutorInfo, PairValueEnum};


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

#[test]
fn test_connect_and_insert() -> Result<(), CommonError>  {
    let p = connect_odbc_db()?;
    let mut item = p.get_owned(())?;
    let conn = item.get_value();
    let timer = std::time::SystemTime::now();

    conn.execute_pair("CREATE TEMP TABLE temp_users (
        id   INT,
        name TEXT
    )", &PairValueEnum::Null)?;

    conn.execute_pair("insert into temp_users (id, name) values (?, ?)", &PairValueEnum::Array(vec![
        PairValueEnum::Int(123),
        PairValueEnum::String(String::from("123")),
    ]))?;

    let data = conn.execute_pair("select id, name || 'd' as name, length(name) + 123.0 as hello from temp_users", &PairValueEnum::Null)?;

    if let PairValueEnum::Map(m) = data {
        assert_eq!(&m.len(), &3);
        if let PairValueEnum::Array(a) = m.get("id").unwrap() {
            assert_eq!(a[0], PairValueEnum::Int(123));
        } else {
            return CommonError::new(&CommonDefaultErrorKind::Etc, "id error").to_result();
        }

        if let PairValueEnum::Array(a) = m.get("hello").unwrap() {
            assert_ne!(a[0], PairValueEnum::Double(0.0));
        } else {
            return CommonError::new(&CommonDefaultErrorKind::Etc, "name error").to_result();
        }

        if let PairValueEnum::Array(a) = m.get("name").unwrap() {
            assert_eq!(a[0], PairValueEnum::String(String::from("123d")));
        } else {
            return CommonError::new(&CommonDefaultErrorKind::Etc, "name error").to_result();
        }
    } else {
        return CommonError::new(&CommonDefaultErrorKind::Etc, "error").to_result();
    }

    Ok(())
}


#[test]
fn test_connect_and_insert_big_null_check() -> Result<(), CommonError>  {
    let p = connect_odbc_db()?;
    let mut item = p.get_owned(())?;
    let conn = item.get_value();
    let timer = std::time::SystemTime::now();

    conn.execute_pair("CREATE TEMP TABLE temp_users (
        id   INT,
        age  int8,
        address varchar(256),
        flag char(3),
        name TEXT
    )", &PairValueEnum::Null)?;

    for idx in 0..10000 {
        conn.execute_pair("insert into temp_users (id, age, address, flag, name)\
         values (?, ?, ?, ?, ?)", &PairValueEnum::Array(vec![
            PairValueEnum::Int(idx),
            PairValueEnum::BigInt(idx as i64+ 1 ),
            PairValueEnum::Null,
            PairValueEnum::String(String::from("Y")),
            PairValueEnum::String(String::from("Testing")),
        ]))?;
    }

    let null_data = conn
        .execute_pair("select case when id % 2 = 0 then 111 else NULL end as null_data from temp_users", &PairValueEnum::Null)?;

    if let PairValueEnum::Map(m) = null_data {
        if let PairValueEnum::Array(a) = m.get("null_data").unwrap() {
            for idx in 0..a.len() {
                if idx % 2 == 0 {
                    assert_eq!(a[idx], PairValueEnum::Int(111));
                } else {
                    assert_eq!(a[idx], PairValueEnum::Null);
                }
            }
        }
    }

    let addr_null_data = conn
        .execute_pair("select address as  null_data, case when address is not null then 123 else 1 end as chk  from temp_users", &PairValueEnum::Null)?;

    if let PairValueEnum::Map(m) = addr_null_data {
        if let PairValueEnum::Array(a) = m.get("chk").unwrap() {
            for idx in 0..a.len() {
                assert_ne!(a[idx], PairValueEnum::Int(123));
            }
        }
        if let PairValueEnum::Array(a) = m.get("null_data").unwrap() {
            for idx in 0..a.len() {
                assert_eq!(a[idx], PairValueEnum::Null);
            }
        }
    }
    Ok(())
}