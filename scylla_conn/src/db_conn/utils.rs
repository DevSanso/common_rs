use std::error::Error;

use scylla::deserialize::{DeserializeRow, DeserializeValue};
use scylla::frame::response::result::{ColumnType,CqlValue};

use common_core::err::{create_error, COMMON_ERROR_CATEGORY, CRITICAL_ERROR};
use common_conn::CommonValue;
use common_conn::err::*;
use scylla::serialize::batch::{BatchValues, BatchValuesIteratorFromIterator};
use scylla::serialize::row::{SerializeRow, SerializedValues};
use scylla::serialize::value::SerializeValue;
use scylla::serialize::SerializationError;

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

pub(crate) struct ScyllaBatchParam {
    val : Vec<CommonValue>
}

impl ScyllaBatchParam {
    fn new(s : Vec<CommonValue>) -> Self {
        ScyllaBatchParam { val : s }
    }

    fn append_batch_value(&self, batch : &mut SerializedValues)-> Result<(), SerializationError> {
        for data in self.val.iter() {
            let _ = match data {
                CommonValue::Int(i) => batch.add_value(&i, &ColumnType::Int),
                CommonValue::Binrary(i) =>batch.add_value(&i, &ColumnType::Blob),
                CommonValue::Double(i) => batch.add_value(&i, &ColumnType::Double),
                CommonValue::String(i) => batch.add_value(&i, &ColumnType::Text),
                CommonValue::Bool(i) =>batch.add_value(&i, &ColumnType::Boolean),
                CommonValue::BigInt(i) => batch.add_value(&i, &ColumnType::BigInt),
                CommonValue::Float(i) => batch.add_value(&i, &ColumnType::Float),
                CommonValue::Null => batch.add_value(&Option::<i64>::None, &ColumnType::BigInt)
            }?;
        }

        Ok(())
    }
    
}

impl SerializeRow for ScyllaBatchParam {
    fn serialize(
        &self,
        _ctx: &scylla::serialize::row::RowSerializationContext<'_>,
        writer: &mut scylla::serialize::writers::RowWriter,
    ) -> Result<(), scylla::serialize::SerializationError> {
        let mut val = SerializedValues::new();
        
        self.append_batch_value(&mut val)?;

        writer.append_serialize_row(&val);
        todo!()
    }

    fn is_empty(&self) -> bool {
       self.val.len() <= 0
    }
}

pub struct ScyllaBatchParams<'a> {
    params : Vec<Box<dyn SerializeRow>>,
    _marker : std::marker::PhantomData<&'a ()>
}

impl<'a> ScyllaBatchParams<'a> {
    pub fn new(s : Vec<Vec<CommonValue>>) -> Self {
        let ps : Vec<Box<dyn SerializeRow>> = s.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(Box::new(ScyllaBatchParam::new(x.clone())));
            acc
        });

        ScyllaBatchParams {
            params : ps,
            _marker : std::marker::PhantomData
        }
    }

    pub fn as_batch_value_iter<'b>(&'b self) -> RealScyllaBatchParam<'b> where 'b : 'a {
        let p : Vec<&'b dyn SerializeRow> = self.params.iter().fold(Vec::new(), |mut acc, x| {
            acc.push(x.as_ref());
            acc
        });
        RealScyllaBatchParam {
            param : p
        }
    } 
}

pub struct RealScyllaBatchParam<'a> {
    pub(self) param : Vec<&'a dyn SerializeRow>
}



impl<'a> BatchValues for RealScyllaBatchParam<'a> {
    type BatchValuesIter<'r> = BatchValuesIteratorFromIterator<std::slice::Iter<'r,&'a dyn SerializeRow>>
    where
        Self: 'r;
        
    fn batch_values_iter(&self) -> Self::BatchValuesIter<'_> {
        BatchValuesIteratorFromIterator::from(self.param.iter())
    }


}
