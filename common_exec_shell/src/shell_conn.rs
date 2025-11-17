use std::io::Read;
use std::time::Duration;
use std::process;
use std::process::Stdio;

use common_err::{CommonError, gen::CommonDefaultErrorKind};
use common_relational_exec::{RelationalExecuteResultSet, RelationalExecutor, RelationalValue};

pub struct LocalShellConnection;

impl LocalShellConnection {
    pub(crate) fn new() -> Self {LocalShellConnection}
}

impl RelationalExecutor<RelationalValue> for LocalShellConnection {
    fn execute(&mut self, query: &'_ str, param: &[RelationalValue]) -> Result<RelationalExecuteResultSet, CommonError> {
        if param.len() < 2 {
            return CommonError::new(&CommonDefaultErrorKind::NotMatchArgs,
                                    "LocalShellConnection - execute not over size 2").to_result();
        }
        
        let cmd = process::Command::new("sh")
            .args(["-c", query])
            .stdout(Stdio::piped())
            .spawn();

        if cmd.is_err() {
            let err = cmd.err().unwrap();
            return CommonError::new(&CommonDefaultErrorKind::SystemCallFail, 
                                    format!("LocalShellConnection - execute,cmd - {}", err)).to_result();
        }

        let output = cmd.unwrap();
        let mut buffer = String::new();

        output.stdout.unwrap().read_to_string(&mut buffer).map_err(|err| {
            CommonError::new(&CommonDefaultErrorKind::SystemCallFail,
                             format!("LocalShellConnection - output - {}", err))
        })?;

        let sep = param[0].to_string();
        let next = param[1].to_string();

        let mut is_first = true;
        let mut first_size = 0;
        let mut ret = RelationalExecuteResultSet::default();

        for line in buffer.split(next.as_str()) {
            let col = line.split(sep.as_str()).fold(Vec::new(), |mut acc, x| {
                acc.push(RelationalValue::String(x.to_string()));
                acc
            });
            
            if is_first {
                for idx in 0..col.len() {
                    ret.cols_name.push(format!("{}",idx));
                }
                is_first = false;
                first_size = ret.cols_name.len();
            }
            
            if first_size != col.len() {
                return CommonError::new(&CommonDefaultErrorKind::NotMatchArgs, format!("LocalShellConnection - \
                    execute,parsing - left[{}] != right[{}]", first_size, col.len())).to_result();
            }
            ret.cols_data.push(col);
        }
        
        Ok(ret)
    }

    fn get_current_time(&mut self) -> Result<Duration, CommonError> {
        let unix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH);
        
        match unix {
            Ok(d) => Ok(d),
            Err(e) => CommonError::new(&CommonDefaultErrorKind::SystemCallFail, 
                                       format!("LocalShellConnection - get_current_time - {:?}", e)).to_result()
        }
    }
}

