mod relational;
mod pair;

use relational::ScyllaRdbFetcherRow;
pub(crate) use pair::ScyllaPairFetcherRow;
pub type ScyllaFetcherRow = ScyllaRdbFetcherRow;