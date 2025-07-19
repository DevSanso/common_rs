use common_conn;
use duckdb_conn;
use postgres_conn;
use scylla_conn;

pub use common_conn::*;

pub enum DatabaseType {
    POSTGRES,
    SCYLLA,
    DUCKDB
}

pub fn create_common_sql_pool(dbtype : DatabaseType, name : String, info : common_conn::CommonSqlConnectionInfo, alloc_size : usize) -> common_conn::CommonSqlConnectionPool {
    match dbtype {
        DatabaseType::POSTGRES => postgres_conn::create_pg_conn_pool(name, info, alloc_size),
        DatabaseType::SCYLLA => scylla_conn::create_scylla_conn_pool(name, vec![info], alloc_size),
        DatabaseType::DUCKDB => duckdb_conn::create_duckdb_conn_pool(name, info, alloc_size),
    }
}