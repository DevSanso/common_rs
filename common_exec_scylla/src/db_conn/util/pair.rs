

use scylla::deserialize::row::DeserializeRow;
use scylla::deserialize::value::DeserializeValue;
use scylla::value::CqlValue;
use scylla::frame::response::result::NativeType;
use scylla::frame::response::result::ColumnType;
use common_err::{CommonError, gen::CommonDefaultErrorKind};
use common_pair_exec::PairValueEnum;

pub(crate) struct ScyllaPairFetcherRow {
    col : Vec<PairValueEnum>,
    catch_err : Option<Result<(), Box<dyn std::error::Error>>>
}

impl ScyllaPairFetcherRow {
    pub fn get_error(&mut self) -> Result<(), CommonError> {
        let t = self.catch_err.take();

        if t.is_some() {
            return t.unwrap().map_err(|e| {
                CommonError::new(&CommonDefaultErrorKind::InvalidApiCall, e.to_string())
            });
        }

        Ok(())
    }

    pub fn clone_col(&self) -> Vec<PairValueEnum> {
        self.col.clone()
    }
    pub fn as_slice(&self) -> &[PairValueEnum] {self.col.as_slice()}

    #[inline]
    fn cast_cql_val_to_comm_int_value(t : &'_ NativeType, cql_value : &'_ CqlValue) -> PairValueEnum {
        match t {
            NativeType::Int => {
                let opt = cql_value.as_int();
                if opt.is_none() {
                    PairValueEnum::Null
                }else {
                    PairValueEnum::Int(opt.unwrap())
                }
            },
            NativeType::TinyInt => {
                let opt = cql_value.as_tinyint();
                if opt.is_none() {
                    PairValueEnum::Null
                }else {
                    PairValueEnum::Int(opt.unwrap() as i32)
                }
            },
            _ => PairValueEnum::Null
        }
    }

    #[inline]
    fn cast_cql_val_to_comm_bigint_value(cql_value : &'_ CqlValue) -> PairValueEnum {
        let opt = cql_value.as_bigint();
        if opt.is_none() {
            PairValueEnum::Null
        }else {
            PairValueEnum::BigInt(opt.unwrap())
        }
    }

    #[inline]
    fn cast_cql_val_to_comm_float_value(cql_value : &'_ CqlValue) -> PairValueEnum {
        let opt = cql_value.as_float();
        if opt.is_none() {
            PairValueEnum::Null
        }else {
            PairValueEnum::Float(opt.unwrap())
        }
    }

    #[inline]
    fn cast_cql_val_to_comm_double_value(cql_value : &'_ CqlValue) -> PairValueEnum {
        let opt = cql_value.as_double();
        if opt.is_none() {
            PairValueEnum::Null
        }else {
            PairValueEnum::Double(opt.unwrap())
        }
    }

    #[inline]
    fn cast_cql_val_to_comm_text_value(cql_value : &'_ CqlValue) -> PairValueEnum {
        let opt = cql_value.as_text();
        if opt.is_none() {
            PairValueEnum::Null
        }else {
            PairValueEnum::String(opt.unwrap().clone())
        }
    }

    #[inline]
    fn cast_cql_val_to_comm_blob_value(cql_value : &'_ CqlValue) -> PairValueEnum {
        let opt = cql_value.as_blob();
        if opt.is_none() {
            PairValueEnum::Null
        }else {
            PairValueEnum::Bin(opt.unwrap().clone())
        }
    }

    #[inline]
    fn cast_cql_val_to_comm_bool_value(cql_value : &'_ CqlValue) -> PairValueEnum {
        let opt = cql_value.as_boolean();
        if opt.is_none() {
            PairValueEnum::Null
        }else {
            PairValueEnum::Bool(opt.unwrap())
        }
    }

    fn cast_data(t : &'_ NativeType, cql_value : &'_ CqlValue) -> Result<PairValueEnum, CommonError> {
        let d = match t {
            NativeType::Int | NativeType::TinyInt => Self::cast_cql_val_to_comm_int_value(t,cql_value),
            NativeType::BigInt => Self::cast_cql_val_to_comm_bigint_value(cql_value),
            NativeType::Boolean => Self::cast_cql_val_to_comm_bool_value(cql_value),
            NativeType::Blob => Self::cast_cql_val_to_comm_blob_value(cql_value),
            NativeType::Text => Self::cast_cql_val_to_comm_text_value(cql_value),
            NativeType::Float => Self::cast_cql_val_to_comm_float_value(cql_value),
            NativeType::Double => Self::cast_cql_val_to_comm_double_value(cql_value),

            _ => return CommonError::new(&CommonDefaultErrorKind::ParsingFail,
                                         format!("copy_response_data - can't cast data type:{:?}", t)).to_result()
        };
        Ok(d)
    }
}

impl DeserializeRow<'_,'_> for ScyllaPairFetcherRow {
    fn type_check(_: &[scylla::frame::response::result::ColumnSpec]) -> Result<(), scylla::deserialize::TypeCheckError> {
        Ok(())
    }

    fn deserialize(row: scylla::deserialize::row::ColumnIterator<'_, '_>) -> Result<Self, scylla::deserialize::DeserializationError> {
        let mut iter = row.into_iter();
        let mut datas = Vec::with_capacity(10);
        let mut catch_err : Result<(), Box<dyn std::error::Error>> = Ok(());

        while let Some(rc) = iter.next() {
            let raw_c = rc?;
            let cql_value = <Option<CqlValue>>::deserialize(raw_c.spec.typ(), raw_c.slice)?;

            if cql_value.is_none() {
                catch_err = Err(Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, "CqlValue is NULL")));
                break;
            }
            let native_t = match raw_c.spec.typ() {
                ColumnType::Native(n) => Ok(n),
                _ =>  Err(Box::new(std::io::Error::new(std::io::ErrorKind::Unsupported, "Only Support Native Type")))
            };

            if native_t.is_err() {
                catch_err = Err(native_t.unwrap_err());
                break;
            }

            let val = Self::cast_data(native_t.unwrap(), &cql_value.unwrap());

            if val.is_err() {
                catch_err = Err(Box::new(val.err().unwrap()));
                break;
            }

            datas.push(val.unwrap());
        }

        Ok(ScyllaPairFetcherRow {
            col : datas, catch_err : Some(catch_err)
        })
    }
}