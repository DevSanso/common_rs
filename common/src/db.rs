use common_conn;
use duckdb_conn;
use postgres_conn;
use scylla_conn;

pub use common_conn::*;

pub enum DatabaseType {
    POSTGRES(CommonSqlConnectionInfo),
    SCYLLA(Vec<CommonSqlConnectionInfo>),
    DUCKDB(CommonSqlConnectionInfo)
}

pub fn create_common_sql_pool(dbtype : DatabaseType, name : String, alloc_size : usize) -> common_conn::CommonSqlConnectionPool {
    match dbtype {
        DatabaseType::POSTGRES(info) => postgres_conn::create_pg_conn_pool(name, info, alloc_size),
        DatabaseType::SCYLLA(infos) => scylla_conn::create_scylla_conn_pool(name, infos, alloc_size),
        DatabaseType::DUCKDB(info) => duckdb_conn::create_duckdb_conn_pool(name, info, alloc_size),
    }
}