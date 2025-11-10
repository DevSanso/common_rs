use std::error::Error;

use scylla::deserialize::row::DeserializeRow;
use scylla::deserialize::value::DeserializeValue;
use scylla::value::CqlValue;
use scylla::frame::response::result::NativeType;
use scylla::frame::response::result::ColumnType;
use common_core::utils::types::SimpleError;
use common_relational_exec::RelationalValue;

pub(super) struct ScyllaFetcherRow {
    col : Vec<RelationalValue>,
    catch_err : Option<Result<(), Box<dyn Error>>>
}

impl ScyllaFetcherRow {
    pub fn get_error(&mut self) -> Result<(), Box<dyn Error>> {
        let t = self.catch_err.take();
        t.unwrap_or_else(|| SimpleError { msg: "catch_err is NULL".to_string() }.to_result())
    }

    pub fn clone_col(&self) -> Vec<RelationalValue> {
        self.col.clone()
    }

    #[inline]
    fn cast_cql_val_to_comm_int_value(t : &'_ NativeType, cql_value : &'_ CqlValue) -> RelationalValue {
        match t {
            NativeType::Int => {
                let opt = cql_value.as_int();
                if opt.is_none() {
                    RelationalValue::Null
                }else {
                    RelationalValue::Int(opt.unwrap())
                }
            },
            NativeType::TinyInt => {
                let opt = cql_value.as_tinyint();
                if opt.is_none() {
                    RelationalValue::Null
                }else {
                    RelationalValue::Int(opt.unwrap() as i32)
                }
            },
            _ => RelationalValue::Null
        }
    }

    #[inline]
    fn cast_cql_val_to_comm_bigint_value(cql_value : &'_ CqlValue) -> RelationalValue {
        let opt = cql_value.as_bigint();
        if opt.is_none() {
            RelationalValue::Null
        }else {
            RelationalValue::BigInt(opt.unwrap())
        }
    }

    #[inline]
    fn cast_cql_val_to_comm_float_value(cql_value : &'_ CqlValue) -> RelationalValue {
        let opt = cql_value.as_float();
        if opt.is_none() {
            RelationalValue::Null
        }else {
            RelationalValue::Float(opt.unwrap())
        }
    }

    #[inline]
    fn cast_cql_val_to_comm_double_value(cql_value : &'_ CqlValue) -> RelationalValue {
        let opt = cql_value.as_double();
        if opt.is_none() {
            RelationalValue::Null
        }else {
            RelationalValue::Double(opt.unwrap())
        }
    }

    #[inline]
    fn cast_cql_val_to_comm_text_value(cql_value : &'_ CqlValue) -> RelationalValue {
        let opt = cql_value.as_text();
        if opt.is_none() {
            RelationalValue::Null
        }else {
            RelationalValue::String(opt.unwrap().clone())
        }
    }

    #[inline]
    fn cast_cql_val_to_comm_blob_value(cql_value : &'_ CqlValue) -> RelationalValue {
        let opt = cql_value.as_blob();
        if opt.is_none() {
            RelationalValue::Null
        }else {
            RelationalValue::Bin(opt.unwrap().clone())
        }
    }

    #[inline]
    fn cast_cql_val_to_comm_bool_value(cql_value : &'_ CqlValue) -> RelationalValue {
        let opt = cql_value.as_boolean();
        if opt.is_none() {
            RelationalValue::Null
        }else {
            RelationalValue::Bool(opt.unwrap())
        }
    }

    fn cast_data(t : &'_ NativeType, cql_value : &'_ CqlValue) -> Result<RelationalValue, Box<dyn Error>> {
        let d = match t {
            NativeType::Int | NativeType::TinyInt => Self::cast_cql_val_to_comm_int_value(t,cql_value),
            NativeType::BigInt => Self::cast_cql_val_to_comm_bigint_value(cql_value),
            NativeType::Boolean => Self::cast_cql_val_to_comm_bool_value(cql_value),
            NativeType::Blob => Self::cast_cql_val_to_comm_blob_value(cql_value),
            NativeType::Text => Self::cast_cql_val_to_comm_text_value(cql_value),
            NativeType::Float => Self::cast_cql_val_to_comm_float_value(cql_value),
            NativeType::Double => Self::cast_cql_val_to_comm_double_value(cql_value),

            _ => return SimpleError {msg : format!("copy_response_data - can't cast data type:{:?}", t)}.to_result()
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
                catch_err = SimpleError{msg :"ScyllaFetcherNew - deserialize CqlValue is None".to_string()}.to_result();
                break;
            }
            let native_t = match raw_c.spec.typ() {
                ColumnType::Native(n) => Ok(n),
                _ =>  SimpleError{msg :"ScyllaFetcherNew - type not support, only support native".to_string()}.to_result()
            };
            
            if native_t.is_err() {
                catch_err = Err(native_t.unwrap_err());
                break;
            }
            
            let val = Self::cast_data(native_t.unwrap(), &cql_value.unwrap());

            if val.is_err() {
                catch_err = SimpleError{msg : format!("deserialize CqlValue is Err:{}", val.unwrap_err())}.to_result();
                break;
            }

            datas.push(val.unwrap());
        }

        Ok(ScyllaFetcherRow {
            col : datas, catch_err : Some(catch_err)
        })
    }
}