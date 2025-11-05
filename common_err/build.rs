use std::io::Write;
use toml::map::Keys;
use toml::Table;

fn write_enum_ident<V>(f : &mut std::fs::File, ks : Keys<'_, String, V>) -> Result<(), std::io::Error> {
    f.write(concat!("pub enum ","CommonErrorList {\n").as_bytes())?;

    ks.for_each(|x| {
        let enum_name = format!("\t{},\n", x);
        f.write(enum_name.as_bytes()).unwrap();
    });

    f.write(concat!("}\n\n").as_bytes())?;
    Ok(())
}

fn write_enum_trait(f : &mut std::fs::File, t : &Table) -> Result<(), std::io::Error> {
    let ks = t.keys();
    
    f.write("impl CommonErrorKind for CommonErrorList {\n".as_bytes())?;

    f.write("\tfn message(&self) -> &'static str {\n\t\tmatch self {\n".as_bytes())?;
    ks.for_each(|x| {
        let v = t.get(x).unwrap();
        let message = v.as_table()
            .unwrap()
            .get("message")
            .unwrap()
            .as_str()
            .unwrap();
        
        let arms = format!("\t\t\tCommonErrorList::{} => \"{}\",\n", x, message);
        f.write(arms.as_bytes()).unwrap();
    });
    f.write(concat!("\t\t}\n").as_bytes())?;
    f.write(concat!("\t}\n").as_bytes())?;
    f.write(concat!("}\n").as_bytes())?;
    
    Ok(())
}

fn main() {
    let file = std::env::var("CARGO_FEATURE_FILE");

    let data = if file.is_err() || file.as_ref().unwrap() == "" {
        Ok(String::from(include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/asset/default.toml"))))
    } else {
        let file_path_ret = std::env::var("COMMON_ERROR_FILE");
        if file_path_ret.is_err() {
            panic!("COMMON_ERROR_FILE environment variable not set");
        }

        let file_path = file_path_ret.unwrap();
        let f = std::fs::read(file_path.clone());
        let data = if f.is_err() {
            Err(format!("can't read file : {}", file_path))
        } else {
            Ok(String::from_utf8_lossy(f.unwrap().as_slice()).to_string())
        };
        data
    };

    let meta = data.unwrap().as_str().parse::<Table>().unwrap();

    let error_list = meta.get("error").unwrap();

    let gen_file = concat!(env!("CARGO_MANIFEST_DIR"), "/src/gen.rs");

    let mut gen_f = std::fs::OpenOptions::new().write(true).create(true).open(gen_file).unwrap();
    gen_f.set_len(0).unwrap();
    gen_f.write("use crate::CommonErrorKind;\n\n".as_bytes()).unwrap();

    let root = error_list.as_table().unwrap();

    write_enum_ident(&mut gen_f, root.keys()).unwrap();
    write_enum_trait(&mut gen_f, &root).unwrap();


}