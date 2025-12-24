use std::collections::HashMap;
use std::fmt::format;
use std::time::Duration;
use redis::{Commands, ConnectionLike, ToRedisArgs, TypedCommands, Cmd, Value};
use common_err::CommonError;
use common_err::gen::CommonDefaultErrorKind;
use common_pair_exec::{PairExecutor, PairValueEnum};

pub struct RedisConnection {
    redis_client : redis::Client,
}

impl RedisConnection {
    pub fn new(addr : &'_ str, user : &'_ str, password : &'_ str, db_name : &'_ str) -> Result<Self, CommonError> {
        let url = format!("redis://{}:{}@{}/{}", user, password, addr, db_name);
        let client = redis::Client::open(url.as_str()).map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, format!("redis connect failed : {:.1024}", e.to_string()))
        })?;

        Ok(RedisConnection { redis_client : client })
    }

    fn set_pair_to_redis_args(mut cmd :Cmd, param : &PairValueEnum) -> Result<Cmd, CommonError> {
        if &PairValueEnum::Null == param {
            Ok(cmd)
        }
        else {
            if let PairValueEnum::Array(arr) = param {
                for x in arr {
                    let convert  = match x{
                        PairValueEnum::Double(d) => cmd.arg(d),
                        PairValueEnum::Int(i) => cmd.arg(i),
                        PairValueEnum::BigInt(b) => cmd.arg(b),
                        PairValueEnum::String(s) => cmd.arg(s),
                        PairValueEnum::Bin(bin) => cmd.arg(bin),
                        PairValueEnum::Float(f) => cmd.arg(f),
                        _ => {
                            return CommonError::new(&CommonDefaultErrorKind::NoSupport,
                                                    format!("not support type : {:?}", x)).to_result();
                        }
                    };
                }
            }

            Ok(cmd)
        }
    }

    fn convert_redis_map_to_pair_map(value : &Vec<(Value, Value)>) -> Result<PairValueEnum, CommonError> {
        let mut map = HashMap::new();
        for (key, value) in value {
            let key_str = match key {
                Value::Int(i) => i.to_string(),
                Value::Double(d) => d.to_string(),
                Value::BulkString(s) => String::from_utf8_lossy(s.as_slice()).to_string(),
                Value::SimpleString(s) => s.clone(),
                _ => {
                    return CommonError::new(&CommonDefaultErrorKind::ParsingFail,
                                        format!("key convert string failed = {:?}", key)).to_result();
                }
            };

            let convert_val = match value {
                Value::Map(m) => {
                    let pair = Self::convert_redis_map_to_pair_map(m).map_err(|e| {
                        CommonError::extend(&CommonDefaultErrorKind::ParsingFail, "map parsing failed", e)
                    })?;

                    pair
                },
                _ => {
                    let pair = Self::convert_redis_value_or_array_to_pair(value).map_err(|e| {
                        CommonError::extend(&CommonDefaultErrorKind::ParsingFail, "map parsing failed", e)
                    })?;
                    pair
                }
            };
        }
        Ok(PairValueEnum::Map(map))
    }

    fn convert_redis_value_or_array_to_pair(value : &Value) -> Result<PairValueEnum, CommonError> {
        let ret = match value {
            Value::Nil => PairValueEnum::Null,
            Value::Okay => PairValueEnum::Null,
            Value::Int(i) => PairValueEnum::BigInt(*i),
            Value::Double(d) => PairValueEnum::Double(*d),
            Value::BulkString(s) => PairValueEnum::String(String::from_utf8_lossy(s.as_slice()).to_string()),
            Value::SimpleString(s) => PairValueEnum::String(s.clone()),
            Value::Boolean(b) => PairValueEnum::Bool(*b),
            Value::Array(a) | Value::Set(a)  => {
                let mut convert_a = Vec::with_capacity(a.len());
                for item in a {
                    convert_a.push(Self::convert_redis_value_or_array_to_pair(item).map_err(|e| {
                        CommonError::extend(&CommonDefaultErrorKind::InvalidApiCall, "Array Type, item convert failed", e)
                    })?);
                }
                PairValueEnum::Array(convert_a)
            },
            _ => {
                return CommonError::new(&CommonDefaultErrorKind::NoSupport, format!("Not supported type {:?}", value)).to_result();
            }
        };
        Ok(ret)
    }

    fn convert_redis_value_to_pair_root_map(value : Value) -> Result<PairValueEnum, CommonError> {
        let ret = match value {
            Value::Nil | Value::Okay => {PairValueEnum::Null},
            Value::Map(m) => {
                let map = Self::convert_redis_map_to_pair_map(&m).map_err(|e| {
                   CommonError::extend(&CommonDefaultErrorKind::ParsingFail, "root parisng error map", e)
                })?;

                map
            },
            Value::ServerError(err) => {
                return CommonError::new(&CommonDefaultErrorKind::ExecuteFail, format!("server error: {:.1024}", err)).to_result();
            },
            _ => {
                let data = Self::convert_redis_value_or_array_to_pair(&value).map_err(|e| {
                    CommonError::extend(&CommonDefaultErrorKind::ParsingFail,
                                        format!("simple type convert failed = {:?}", value), e)
                })?;
                let mut map = HashMap::new();
                map.insert("0".to_string(), data);
                PairValueEnum::Map(map)
            }
        };

        Ok(ret)
    }

}

impl PairExecutor for RedisConnection {
    fn execute_pair(&mut self, query: &'_ str, param: &PairValueEnum) -> Result<PairValueEnum, CommonError> {
        let cmd = Self::set_pair_to_redis_args(redis::cmd(query), param).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::ThirdLibCallFail, "set args failed", e)
        })?;

        let result : Value = cmd.query(&mut self.redis_client).map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::ExecuteFail, format!("execute: {}", e.to_string()))
        })?;

        let ret = Self::convert_redis_value_to_pair_root_map(result).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::ExecuteFail, "convert data failed", e)
        })?;

        Ok(ret)
    }

    fn get_current_time(&mut self) -> Result<Duration, CommonError> {
        let execute_result = self.execute_pair("time", &PairValueEnum::Null).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::ExecuteFail, "get current time failed", e)
        })?;
        if let PairValueEnum::Map(data) = execute_result {
            if let Some(PairValueEnum::Array(a)) = data.get("0") {
                if a.len() != 2 {
                    CommonError::new(&CommonDefaultErrorKind::Etc, format!("data size 2 != {}", a.len())).to_result()
                } else {
                    let sec = if let PairValueEnum::String(second) = &a[0] {
                        Ok(second.as_str().parse::<i64>().map_err(|e| {
                            CommonError::new(&CommonDefaultErrorKind::Etc, format!("parse time error, second: {}", e.to_string()))
                        })?)
                    } else {
                        CommonError::new(&CommonDefaultErrorKind::Etc,
                                         format!("not second bulk string type : {:?}", a[0])).to_result()
                    }?;

                    let micro = if let PairValueEnum::String(micro) = &a[1] {
                        Ok(micro.as_str().parse::<i64>().map_err(|e| {
                            CommonError::new(&CommonDefaultErrorKind::Etc, format!("parse time error, second: {}", e.to_string()))
                        })?)
                    } else {
                        CommonError::new(&CommonDefaultErrorKind::Etc,
                                         format!("not second bulk string type : {:?}", a[0])).to_result()
                    }?;

                    Ok(Duration::new(sec as u64, (micro * 1000) as u32))
                }
            } else {
                CommonError::new(&CommonDefaultErrorKind::Etc, format!("not array type : {:?}", data)).to_result()
            }
        }
        else {
            CommonError::new(&CommonDefaultErrorKind::ExecuteFail, format!("not map type : {:?}", execute_result)).to_result()
        }
    }
}