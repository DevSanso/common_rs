use std::error::Error;
use std::io::Read;
use std::time::Duration;
use std::process;
use std::process::Stdio;

use common_core::utils::types::SimpleError;
use common_relational_exec::{RelationalExecuteResultSet, RelationalExecutor, RelationalValue};

use crate::ShellParam;
pub struct LocalShellConnection;

impl LocalShellConnection {
    pub(crate) fn new() -> Self {LocalShellConnection}
}

impl RelationalExecutor<ShellParam> for LocalShellConnection {
    fn execute(&mut self, query: &'_ str, param: &[ShellParam]) -> Result<RelationalExecuteResultSet, Box<dyn Error>> {
        if param.len() < 1 {
            return SimpleError { msg : "LocalShellConnection - execute - param is not exists".to_string()}
                .to_result();
        }

        let cmdline = query.split(" ").collect::<Vec<&str>>();

        let cmd = process::Command::new(cmdline[0])
            .args(&cmdline[1..])
            .stdout(Stdio::piped())
            .spawn();

        if cmd.is_err() {
            let err = cmd.err().unwrap();
            return SimpleError { msg : format!("LocalShellConnection - execute,cmd - {}", err)}.to_result();
        }

        let output = cmd.unwrap();
        let mut buffer = String::new();

        output.stdout.unwrap().read_to_string(&mut buffer).map_err(|err| {
            SimpleError { msg: format!("LocalShellConnection - execute,output - {}", err) }
                .to_result::<(), Box<dyn Error>>()
                .unwrap_err()
        })?;

        let sep = &param[0].sep;
        let next = &param[0].next;

        let mut is_first = true;
        let mut first_size = 0;
        let mut ret = RelationalExecuteResultSet::default();

        for line in buffer.split(next) {
            let col = line.split(sep).fold(Vec::new(), |mut acc, x| {
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
                return SimpleError {msg : format!("LocalShellConnection - \
                    execute,parsing - left[{}] != right[{}]", first_size, col.len())}.to_result();
            }
            ret.cols_data.push(col);
        }
        
        Ok(ret)
    }

    fn get_current_time(&mut self) -> Result<Duration, Box<dyn Error>> {
        let unix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH);
        
        match unix {
            Ok(d) => Ok(d),
            Err(e) => SimpleError { msg: 
                format!("LocalShellConnection - get_current_time - {:?}", e) }.to_result(),
        }
    }
}

