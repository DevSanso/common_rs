use std::error::Error;

use scylla::deserialize::{DeserializeRow, DeserializeValue};
use scylla::frame::response::result::{ColumnType,CqlValue};

use common_core::err::{create_error, COMMON_ERROR_CATEGORY, CRITICAL_ERROR};
use common_conn::CommonValue;
use common_conn::err::*;

pub(super) struct ScyllaFetcherRow {
    col : Vec<CommonValue>,
    catch_err : Option<Result<(), Box<dyn Error>>>
}

impl ScyllaFetcherRow {
    pub fn get_error(&mut self) -> Result<(), Box<dyn Error>> {
        let t = self.catch_err.take();
        match t {
            Some(s) => s,
            None => create_error(COMMON_ERROR_CATEGORY, CRITICAL_ERROR
                , "catch_err is NULL".to_string(), None).as_error()
        }
    }

    pub fn clone_col(&self) -> Vec<CommonValue> {
        self.col.clone()
    }

    #[inline]
    fn cast_cql_val_to_comm_int_value(t : &'_ ColumnType, cql_value : &'_ CqlValue) -> CommonValue {
        match t {
            ColumnType::Int => {
                let opt = cql_value.as_int();
                if opt.is_none() {
                    CommonValue::Null
                }else {
                    CommonValue::Int(opt.unwrap())
                }
            },
            ColumnType::TinyInt => {
                let opt = cql_value.as_tinyint();
                if opt.is_none() {
                    CommonValue::Null
                }else {
                    CommonValue::Int(opt.unwrap() as i32)
                }
            },
            _ => CommonValue::Null
        }
    }

    #[inline]
    fn cast_cql_val_to_comm_bigint_value(cql_value : &'_ CqlValue) -> CommonValue {
        let opt = cql_value.as_bigint();
        if opt.is_none() {
            CommonValue::Null
        }else {
            CommonValue::BigInt(opt.unwrap())
        }
    }

    #[inline]
    fn cast_cql_val_to_comm_float_value(cql_value : &'_ CqlValue) -> CommonValue {
        let opt = cql_value.as_float();
        if opt.is_none() {
            CommonValue::Null
        }else {
            CommonValue::Float(opt.unwrap())
        }
    }

    #[inline]
    fn cast_cql_val_to_comm_double_value(cql_value : &'_ CqlValue) -> CommonValue {
        let opt = cql_value.as_double();
        if opt.is_none() {
            CommonValue::Null
        }else {
            CommonValue::Double(opt.unwrap())
        }
    }

    #[inline]
    fn cast_cql_val_to_comm_text_value(cql_value : &'_ CqlValue) -> CommonValue {
        let opt = cql_value.as_text();
        if opt.is_none() {
            CommonValue::Null
        }else {
            CommonValue::String(opt.unwrap().clone())
        }
    }

    #[inline]
    fn cast_cql_val_to_comm_blob_value(cql_value : &'_ CqlValue) -> CommonValue {
        let opt = cql_value.as_blob();
        if opt.is_none() {
            CommonValue::Null
        }else {
            CommonValue::Binrary(opt.unwrap().clone())
        }
    }

    #[inline]
    fn cast_cql_val_to_comm_bool_value(cql_value : &'_ CqlValue) -> CommonValue {
        let opt = cql_value.as_boolean();
        if opt.is_none() {
            CommonValue::Null
        }else {
            CommonValue::Bool(opt.unwrap())
        }
    }

    fn cast_data(t : &'_ ColumnType, cql_value : &'_ CqlValue) -> Result<CommonValue, Box<dyn Error>> {
        let d = match t {
            ColumnType::Int | ColumnType::TinyInt => Self::cast_cql_val_to_comm_int_value(t,cql_value),
            ColumnType::BigInt => Self::cast_cql_val_to_comm_bigint_value(cql_value),
            ColumnType::Boolean => Self::cast_cql_val_to_comm_bool_value(cql_value),
            ColumnType::Blob => Self::cast_cql_val_to_comm_blob_value(cql_value),
            ColumnType::Text => Self::cast_cql_val_to_comm_text_value(cql_value),
            ColumnType::Float => Self::cast_cql_val_to_comm_float_value(cql_value),
            ColumnType::Double => Self::cast_cql_val_to_comm_double_value(cql_value),
            
            _ => return create_error(COMMON_CONN_ERROR_CATEGORY, 
                RESPONSE_SCAN_ERROR, 
                format!("copy_reponse_data - can't cast data type:{:?}", t), None).as_error()
         };
        Ok(d)
    }
}

impl DeserializeRow<'_,'_> for ScyllaFetcherRow {
    fn type_check(_: &[scylla::frame::response::result::ColumnSpec]) -> Result<(), scylla::deserialize::TypeCheckError> {
        Ok(())
    }

    fn deserialize(row: scylla::deserialize::row::ColumnIterator<'_, '_>) -> Result<Self, scylla::deserialize::DeserializationError> {
        let mut iter = row.into_iter();
        let mut datas = Vec::with_capacity(10);
        let mut catch_err : Result<(), Box<dyn Error>> = Ok(());

        while let Some(rc) = iter.next() {
            let raw_c = rc?;
            let cql_value = <Option<CqlValue>>::deserialize(raw_c.spec.typ(), raw_c.slice)?;
            
            if cql_value.is_none() { 
                catch_err = create_error(COMMON_CONN_ERROR_CATEGORY, 
                    RESPONSE_SCAN_ERROR, 
                    format!("ScyllaFetcherNew - deserialize CqlValue is None"), None).as_error();
                break;
            }

            let val = Self::cast_data(raw_c.spec.typ(), &cql_value.unwrap());

            if val.is_err() {
                catch_err = create_error(COMMON_CONN_ERROR_CATEGORY, 
                    RESPONSE_SCAN_ERROR, 
                    format!("ScyllaFetcherNew - cast_data error"), Some(val.err().unwrap())).as_error();
                break;
            }

            datas.push(val.unwrap());
        }

        Ok(ScyllaFetcherRow {
            col : datas, catch_err : Some(catch_err)
        })
    }
}