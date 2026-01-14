use std::ffi::c_void;
use std::time::Duration;
use std::ptr;
use std::collections::HashMap;
use common_err::CommonError;
use common_pair_exec::{PairExecutor, PairValueEnum};
use odbc_sys::*;
use common_err::gen::CommonDefaultErrorKind;
use common_pair_exec::PairValueEnum::Null;

pub struct OdbcConnection {
    env_h : Handle,
    conn_h : Handle,

    current_time_query : String,
    current_time_col_name : String
}

unsafe fn get_odbc_diagnostics(handle_type: HandleType, handle: Handle) -> String {
    let mut rec_number: SmallInt = 1;
    let mut string_buffer = String::with_capacity(1024);
    use std::fmt::Write;

    loop {
        let mut sql_state: [Char; 6] = [0; 6]; // 5 + NULL
        let mut native_error: Integer = 0;
        let mut message_text: [Char; 512] = [0; 512];
        let mut text_length: SmallInt = 0;

        let ret = SQLGetDiagRec(
            handle_type,
            handle,
            rec_number,
            sql_state.as_mut_ptr(),
            &mut native_error,
            message_text.as_mut_ptr(),
            message_text.len() as SmallInt,
            &mut text_length,
        );

        if ret == SqlReturn::NO_DATA {
            break;
        }

        if ret == SqlReturn::SUCCESS || ret == SqlReturn::SUCCESS_WITH_INFO {
            let state = std::ffi::CStr::from_ptr(sql_state.as_ptr() as *const i8)
                .to_string_lossy();

            let message = std::ffi::CStr::from_ptr(message_text.as_ptr() as *const i8)
                .to_string_lossy();

            writeln!(string_buffer,
                     "[ODBC DIAG {}] SQLSTATE={}, NativeError={}, Message={}",
                     rec_number, state, native_error, message).expect("odbc error message writer is broken");
        } else {
            break;
        }

        rec_number += 1;
    }

    string_buffer
}
struct ODBCStmt {
    stmt_h : Handle
}

impl ODBCStmt {
    fn new(stmt_h : Handle) -> ODBCStmt {
        ODBCStmt {
            stmt_h
        }
    }
    fn cast_sql_type_to_ctype(sql_t : &SqlDataType) -> Result<CDataType, CommonError> {
        match *sql_t {
            SqlDataType::DOUBLE => Ok(CDataType::Double),
            SqlDataType::CHAR => Ok(CDataType::Char),
            SqlDataType::INTEGER => Ok(CDataType::SLong),
            SqlDataType::SMALLINT => Ok(CDataType::SLong),
            SqlDataType::FLOAT => Ok(CDataType::Float),
            SqlDataType::DECIMAL => Ok(CDataType::Double),
            SqlDataType::NUMERIC => Ok(CDataType::Double),
            SqlDataType::EXT_BIG_INT => Ok(CDataType::SBigInt),
            _ => {
                CommonError::new(&CommonDefaultErrorKind::NoSupport, format!("not support {:?}", *sql_t)).to_result()
            }
        }
    }
    #[inline]
    unsafe fn bind_stmt(stmt_h : Handle, param : &'_ [&'_ PairValueEnum]) -> Result<(), CommonError> {
        for idx in 0..param.len() {
            let bind_data = match param[idx] {
                PairValueEnum::Double(d) => {
                    (CDataType::Double, SqlDataType::DOUBLE, 0, 5, d as *const f64 as *mut c_void)
                }
                PairValueEnum::Int(i) => {
                    (CDataType::SLong, SqlDataType::INTEGER, 0, 0, i as *const i32 as *mut c_void)
                }
                PairValueEnum::BigInt(bi) => {
                    (CDataType::SBigInt, SqlDataType::DECIMAL, 0, 0, bi as *const i64 as *mut c_void)
                }
                PairValueEnum::String(s) => {
                    (CDataType::Char, SqlDataType::VARCHAR, s.len() as isize, 0, s.as_ptr() as *mut c_void)
                }
                PairValueEnum::Float(f) => {
                    (CDataType::Float, SqlDataType::FLOAT,  0, 5, f as *const f32 as *mut c_void)
                }
                PairValueEnum::Null => {
                    (CDataType::Char, SqlDataType::VARCHAR, NTS, 0,"\0".as_ptr() as *mut c_void )
                }
                _ => {
                    return CommonError::new(&CommonDefaultErrorKind::NoSupport, format!("not support : {:?}", param[idx])).to_result()
                }
            };

            let ret = SQLBindParameter(
                stmt_h.clone().as_hstmt(),
                (idx + 1) as USmallInt,
                ParamType::Input,
                bind_data.0,
                bind_data.1,
                bind_data.2 as ULen,
                bind_data.3,
                bind_data.4,
                0,
                ptr::null_mut(),
            );

            if ret != SqlReturn::SUCCESS && ret != SqlReturn::SUCCESS_WITH_INFO {
                let error_str = get_odbc_diagnostics(HandleType::Stmt, stmt_h.clone());
                return CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, error_str).to_result();
            }
        }

        Ok(())
    }

    unsafe fn get_cols_meta_data(stmt_h : Handle, count : usize) -> Result<Vec<(String, SqlDataType, usize)>, CommonError> {
        let stmt = stmt_h.as_hstmt();
        let mut v = Vec::with_capacity(count);

        for idx in 0..count {
            let mut col_name : [Char; 256] = [0;256];
            let mut col_name_length : SmallInt = 1;
            let mut col_len : ULen = 0;
            let mut data_type = SqlDataType::INTEGER;
            let mut dummy1 : SmallInt = 0;
            let mut dummy2 : Nullability = Nullability::NULLABLE;
            let ret = SQLDescribeCol(stmt,
                           idx as USmallInt + 1,
                           col_name.as_mut_ptr(),
                           255,
                           &mut col_name_length as *mut SmallInt,
                           &mut data_type as *mut SqlDataType,
                           &mut col_len as *mut ULen,
                           &mut dummy1 as *mut SmallInt,
                           &mut dummy2 as *mut Nullability

            );

            if ret != SqlReturn::SUCCESS && ret != SqlReturn::SUCCESS_WITH_INFO {
                let error_str = get_odbc_diagnostics(HandleType::Stmt, stmt_h.clone());
                return CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, error_str).to_result();
            }
            let s = String::from_raw_parts(col_name.as_mut_ptr() as *mut u8, col_name_length as usize, 1);
            v.push((s,data_type, col_len as usize));
        }

        Ok(v)
    }

    unsafe fn execute(&mut self, query : &'_ str, param : Option<&'_ [PairValueEnum]>) -> Result<PairValueEnum, CommonError> {
        if param.is_some() {
            let p_ref : Vec<&PairValueEnum> = param.unwrap().iter().map(|p| p).collect();
            Self::bind_stmt(self.stmt_h.clone(), p_ref.as_slice()).map_err(|e| {
                CommonError::extend(&CommonDefaultErrorKind::ExecuteFail, "bind failed", e)
            })?;
        }

        let mut col_buffer = HashMap::with_capacity(3);

        let real_stmt = self.stmt_h.clone().as_hstmt();

        let ret = SQLExecDirect(real_stmt, query.as_ptr(), NTS as Integer);

        if ret != SqlReturn::SUCCESS && ret != SqlReturn::SUCCESS_WITH_INFO {
            let error_str = get_odbc_diagnostics(HandleType::Stmt, self.stmt_h.clone());
            return CommonError::new(&CommonDefaultErrorKind::ExecuteFail, error_str).to_result();
        }
        let mut cols_count = 0 as SmallInt;
        let ret = SQLNumResultCols(real_stmt, &mut cols_count as *mut SmallInt);

        if  ret != SqlReturn::SUCCESS && ret != SqlReturn::SUCCESS_WITH_INFO {
            let error_str = get_odbc_diagnostics(HandleType::Stmt, self.stmt_h.clone());
            return CommonError::new(&CommonDefaultErrorKind::ExecuteFail, error_str).to_result();
        }

        if cols_count <= 0 {
            return Ok(PairValueEnum::Null);
        }

        let cols = Self::get_cols_meta_data(self.stmt_h.clone(), cols_count as usize).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::ExecuteFail, "get cols failed", e)
        })?;

        for col in &cols {
            col_buffer.insert(col.0.clone(), Vec::with_capacity(5));
        }

        loop {
            let ret = SQLFetch(real_stmt);
            if ret == SqlReturn::NO_DATA {
                if col_buffer.is_empty() {
                    return Ok(PairValueEnum::Null);
                }
                break;
            }

            if ret != SqlReturn::SUCCESS && ret != SqlReturn::SUCCESS_WITH_INFO {
                let error_str = get_odbc_diagnostics(HandleType::Stmt, self.stmt_h.clone());
                return CommonError::new(&CommonDefaultErrorKind::FetchFailed, error_str).to_result();
            }

            for idx in 0..cols.len() {
                let ctype = Self::cast_sql_type_to_ctype(&cols[idx].1).map_err(|e|{
                    CommonError::extend(&CommonDefaultErrorKind::Etc, "", e)
                })?;
                let mut is_chk_null_len = Len::default();

                let mut int_buffer : libc::c_int = 0;
                let mut double_buffer : libc::c_double = 0.0;
                let mut float_buffer : libc::c_float = 0.0;
                let mut bigint_buffer : libc::c_longlong = 0;
                let mut string_buffer : Vec<libc::c_char> = vec![0; cols[idx].2];

                let buffer_ptr = match ctype {
                    CDataType::SLong => {
                        Ok((&mut int_buffer as *mut libc::c_int) as Pointer)
                    },
                    CDataType::Float => {
                        Ok((&mut float_buffer as *mut libc::c_float) as Pointer)
                    },
                    CDataType::Double => {
                        Ok((&mut double_buffer as *mut libc::c_double) as Pointer)
                    },
                    CDataType::Char => {
                        Ok(string_buffer.as_mut_ptr()  as Pointer)
                    },
                    CDataType::SBigInt => {
                        Ok((&mut bigint_buffer as *mut libc::c_longlong) as Pointer)
                    },
                    _ => {
                        CommonError::new(&CommonDefaultErrorKind::Etc, format!("not support {:?}", ctype)).to_result()
                    }
                }?;

                let data_ret = SQLGetData(
                    real_stmt,
                    idx  as USmallInt + 1,
                    ctype,
                    buffer_ptr,
                    cols[idx].2 as Len,
                    &mut is_chk_null_len as *mut Len);

                if data_ret != SqlReturn::SUCCESS && data_ret != SqlReturn::SUCCESS_WITH_INFO {
                    let error_str = get_odbc_diagnostics(HandleType::Stmt, self.stmt_h.clone());
                    return CommonError::new(&CommonDefaultErrorKind::FetchFailed, error_str).to_result();
                }

                if is_chk_null_len < 0 {
                    col_buffer.get_mut(&cols[idx].0)
                        .expect(format!("broken col data name {}", &cols[idx].0).as_str())
                        .push(PairValueEnum::Null);
                } else {
                    match ctype {
                        CDataType::SLong => {
                            col_buffer.get_mut(&cols[idx].0)
                                .expect(format!("broken col data name {}", &cols[idx].0).as_str())
                                .push(PairValueEnum::Int(int_buffer));
                        },
                        CDataType::Float => {
                            col_buffer.get_mut(&cols[idx].0)
                                .expect(format!("broken col data name {}", &cols[idx].0).as_str())
                                .push(PairValueEnum::Float(float_buffer));
                        },
                        CDataType::Double => {
                            col_buffer.get_mut(&cols[idx].0)
                                .expect(format!("broken col data name {}", &cols[idx].0).as_str())
                                .push(PairValueEnum::Double(double_buffer));
                        },
                        CDataType::SBigInt => {
                            col_buffer.get_mut(&cols[idx].0)
                                .expect(format!("broken col data name {}", &cols[idx].0).as_str())
                                .push(PairValueEnum::BigInt(bigint_buffer));
                        },
                        CDataType::Char => {
                            let s = String::from_utf8(
                                string_buffer.iter().map(|&c| c as u8).collect::<Vec<u8>>()).map_err(|e| {
                                CommonError::new(&CommonDefaultErrorKind::SystemCallFail, format!("{}", e))
                            })?;

                            col_buffer.get_mut(&cols[idx].0)
                                .expect(format!("broken col data name {}", &cols[idx].0).as_str())
                                .push(PairValueEnum::String(s));
                        },
                        _ => {
                            panic!("not support {:?}, broken ODBCStmt execute code", ctype);
                        }
                    };
                }
            }
        }

        Ok(PairValueEnum::Map(col_buffer.iter().fold(HashMap::with_capacity(col_buffer.len()), |mut acc,x| {
            acc.insert(x.0.clone(), PairValueEnum::Array(x.1.clone()));
            acc
        })))
    }
}

impl Drop for ODBCStmt {
    fn drop(&mut self) {
        unsafe {
            let _ = SQLFreeHandle(HandleType::Stmt, self.stmt_h);
        }
    }
}

impl OdbcConnection {

    pub fn new(data_source : String, current_time_query : String, current_time_col_name : String) -> Result<Self, CommonError> {
        let mut env_h: Handle = Handle::null();
        let mut conn_h : Handle = Handle::null();

        unsafe {
            let ret = SQLAllocHandle(HandleType::Env, Handle::null(), &mut env_h);
            if ret != SqlReturn::SUCCESS && ret != SqlReturn::SUCCESS_WITH_INFO {
                return CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, "odbc get failed env handle").to_result();
            }

            let ret = SQLSetEnvAttr(env_h.clone().as_henv(), EnvironmentAttribute::OdbcVersion, Pointer::from(AttrOdbcVersion::Odbc3), Integer::default());
            if ret != SqlReturn::SUCCESS && ret != SqlReturn::SUCCESS_WITH_INFO {
                let _ = SQLFreeHandle(HandleType::Env, env_h);
                return CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, "odbc set failed odbc version").to_result();
            }

            let ret = SQLAllocHandle(HandleType::Dbc, env_h.clone(), &mut conn_h);
            if ret != SqlReturn::SUCCESS && ret != SqlReturn::SUCCESS_WITH_INFO {
                let _ = SQLFreeHandle(HandleType::Env, env_h);
                return CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, "odbc get failed dbc handle").to_result();
            }
            let ret = SQLDriverConnect(conn_h.clone().as_hdbc(),
                                       ptr::null_mut(),
                                       data_source.as_ptr(),
                                       NTS as SmallInt,
                                       ptr::null_mut(),
                                       0 as SmallInt,
                                       ptr::null_mut(),
                                       DriverConnectOption::NoPrompt);

            if ret != SqlReturn::SUCCESS && ret != SqlReturn::SUCCESS_WITH_INFO {
                let error_str = get_odbc_diagnostics(HandleType::Dbc, conn_h.clone());
                let _ = SQLFreeHandle(HandleType::Dbc, conn_h);
                let _ = SQLFreeHandle(HandleType::Env, env_h);
                return CommonError::new(&CommonDefaultErrorKind::ConnectFail, format!("odbc conn failed:\n\t{}", error_str)).to_result();
            }
        }
        Ok(OdbcConnection {
            env_h,
            conn_h,
            current_time_query,
            current_time_col_name
        })
    }
}

impl Drop for OdbcConnection {
    fn drop(&mut self) {
        unsafe {
            let _ = SQLDisconnect(self.conn_h.clone().as_hdbc());
            let _ = SQLFreeHandle(HandleType::Dbc, self.conn_h);
            let _ = SQLFreeHandle(HandleType::Env, self.env_h);
        }
    }
}

impl PairExecutor for OdbcConnection {
    fn execute_pair(&mut self, query: &'_ str, param: &PairValueEnum) -> Result<PairValueEnum, CommonError> {
        let mut stmt = unsafe {
            let mut stmt : Handle = Handle::null();

            let ret = SQLAllocHandle(HandleType::Stmt, self.conn_h.clone(), &mut stmt);
            if ret != SqlReturn::SUCCESS && ret != SqlReturn::SUCCESS_WITH_INFO {
                return CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, "STMT ALLOC FAILED").to_result();
            }

            ODBCStmt::new(stmt)
        };

       let data = unsafe {
            if let PairValueEnum::Array(arr) = param {
                stmt.execute(query, Some(arr.as_slice()))
            } else {
                stmt.execute(query, None)
            }
        }.map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::ExecuteFail, "", e)
        })?;

        Ok(data)
    }

    fn get_current_time(&mut self) -> Result<Duration, CommonError> {
        let query = self.current_time_query.clone();
        let ret = self.execute_pair(query.as_str(), &Null).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::ExecuteFail, "", e)
        })?;

        match ret {
            PairValueEnum::Null => {
                CommonError::new(&CommonDefaultErrorKind::NoData, "get current time is null").to_result()
            },
            PairValueEnum::Map(m) => {
                let pair = m.get(&self.current_time_col_name);
                if let Some(PairValueEnum::BigInt(d)) = pair {
                    Ok(Duration::from_millis(*d as u64))
                } else {
                    CommonError::new(&CommonDefaultErrorKind::Etc, "get data failed").to_result()
                }
            },
            _ => CommonError::new(&CommonDefaultErrorKind::NoData, "not support data type").to_result()
        }
    }
}