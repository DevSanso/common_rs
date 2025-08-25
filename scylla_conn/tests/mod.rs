

#[cfg(test)]
mod scylla_tests {
    use common_conn::{CommonSqlConnection, CommonSqlConnectionBox, CommonSqlConnectionInfo, CommonValue};
    use common_core::collection::pool::PoolItem;
    use std::error::Error;
    use scylla_conn::create_scylla_conn_pool;

    fn init_testing_db_delete(c : &mut Box<dyn CommonSqlConnection>) -> Result<(), Box<dyn Error>> {
        c.execute("DROP TABLE IF EXISTS test_db.users", &[])?;
        c.execute("DROP KEYSPACE IF EXISTS test_db", &[])?;
        Ok(())
    }

    fn init_testing_db_create(c : &mut Box<dyn CommonSqlConnection>) -> Result<(), Box<dyn Error>> {
        c.execute("CREATE  KEYSPACE test_db 
                WITH REPLICATION = {
                'class': 'SimpleStrategy',
                'replication_factor': 1
                }", &[])?;
        
        c.execute("CREATE TABLE test_db.users (
            k INT,
            id UUID,
            username TEXT,
            email TEXT,
            age INT,
            created_at TIMESTAMP,
            is_active BOOLEAN,
            PRIMARY KEY (k, age)
        ) WITH CLUSTERING ORDER BY (age ASC)", &[])?;
        Ok(())
    }

    fn init_testing_db(mut conn : CommonSqlConnectionBox) -> Result<(), Box<dyn Error>> {
        let real_conn: &mut Box<dyn CommonSqlConnection> = conn.get_value();
        init_testing_db_delete(real_conn)?;
        init_testing_db_create(real_conn)?;
        Ok(())
    }

    #[test]
    fn tests_connect_scylla_db() -> Result<(), Box<dyn Error>>{
        let conn = create_scylla_conn_pool("Testing".to_string(), vec![CommonSqlConnectionInfo {
            addr : "localhost:9042".to_string(),
            db_name : "system".to_string(),
            user : "cassandra".to_string(),
            password : "cassandra".to_string(),
            timeout_sec : 10
        }], 10);


        let c = conn.get_owned(())?;
        Ok(())
    }

    #[test]
    fn tests_get_current_scylla_db() -> Result<(), Box<dyn Error>>{
        let conn = create_scylla_conn_pool("Testing".to_string(), vec![CommonSqlConnectionInfo {
            addr : "localhost:9042".to_string(),
            db_name : "system".to_string(),
            user : "cassandra".to_string(),
            password : "cassandra".to_string(),
            timeout_sec : 10
        }], 10);


        let mut c = conn.get_owned(())?;
        let real = c.get_value();
        let current = real.get_current_time()?;
        assert_ne!(0, current.as_secs());
        
        Ok(())
    }

    #[test]
    fn tests_init_query_scylla_db() -> Result<(), Box<dyn Error>>{
        let conn = create_scylla_conn_pool("Testing".to_string(), vec![CommonSqlConnectionInfo {
            addr : "localhost:9042".to_string(),
            db_name : "system".to_string(),
            user : "cassandra".to_string(),
            password : "cassandra".to_string(),
            timeout_sec : 10
        }], 10);


        let c = conn.get_owned(())?;
        
        init_testing_db(c)?;
        Ok(())
    }

    #[test]
    fn tests_insert_and_select_query_scylla_db() -> Result<(), Box<dyn Error>> {
        tests_init_query_scylla_db()?;
        let conn = create_scylla_conn_pool("Testing".to_string(), vec![CommonSqlConnectionInfo {
            addr : "localhost:9042".to_string(),
            db_name : "test_db".to_string(),
            user : "cassandra".to_string(),
            password : "cassandra".to_string(),
            timeout_sec : 10
        }], 10);

        let mut real_conn = conn.get_owned(())?;
        {
            let executor = real_conn.get_value();
            executor.execute("INSERT INTO users (k, id, username, email, age, created_at, is_active) 
                    VALUES (1, uuid(), ?, ?, ?, toTimestamp(now()), true)", 
                    &[CommonValue::String("testing".to_string()), 
                    CommonValue::String("testing@testing.com".to_string()),
                    CommonValue::Int(123)])?;

            executor.execute("INSERT INTO users (k, id, username, email, age, created_at, is_active) 
                    VALUES (1, uuid(), ?, ?, ?, toTimestamp(now()), true)", 
                    &[CommonValue::String("testing2".to_string()), 
                    CommonValue::String("testing2@testing.com".to_string()),
                    CommonValue::Int(124)])?;

            let ret = executor.execute(
                " SELECT age, email, username FROM users where k = 1 ", &[])?;
                
            {
                let first = &ret.cols_data[0];
                let name = match  &first[2] {
                    CommonValue::String(s) => s,
                    _ => panic!("convert first 0 failed")
                };
                let email = match  &first[1] {
                    CommonValue::String(s) => s,
                    _ => panic!("convert first 0 failed")
                };
                let age = match  &first[0] {
                    CommonValue::Int(s) => s,
                    _ => panic!("convert first 0 failed")
                };

                assert_eq!("testing", name.as_str());
                assert_eq!("testing@testing.com", email.as_str());
                assert_eq!(123, *age);

            }
            {
                let first = &ret.cols_data[1];
                let name = match  &first[2] {
                    CommonValue::String(s) => s,
                    _ => panic!("convert first 0 failed")
                };
                let email = match  &first[1] {
                    CommonValue::String(s) => s,
                    _ => panic!("convert first 0 failed")
                };
                let age = match  &first[0] {
                    CommonValue::Int(s) => s,
                    _ => panic!("convert first 0 failed")
                };

                assert_eq!("testing2", name.as_str());
                assert_eq!("testing2@testing.com", email.as_str());
                assert_eq!(124, *age);

            }
        }

        Ok(())
    }

} 