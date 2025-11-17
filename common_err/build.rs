use std::io::Write;
use toml::map::Keys;
use toml::Table;

fn write_enum_ident<V>(f : &mut std::fs::File,  enum_name : &'_ str, ks : Keys<'_, String, V>) -> Result<(), std::io::Error> {
    f.write(format!("pub enum {} {{\n", enum_name).as_bytes())?;
    
    ks.for_each(|x| {
        let enum_name = format!("\t{},\n", x);
        f.write(enum_name.as_bytes()).unwrap();
    });

    f.write(concat!("}\n\n").as_bytes())?;
    Ok(())
}

fn write_enum_trait(f : &mut std::fs::File, enum_name : &'_ str, t : &Table) -> Result<(), std::io::Error> {
    let ks = t.keys();
    
    f.write(format!("impl CommonErrorKind for {} {{\n", enum_name).as_bytes())?;

    f.write("\tfn message(&self) -> &'static str {\n\t\tmatch self {\n".as_bytes())?;
    ks.for_each(|x| {
        let v = t.get(x).unwrap();
        let message = v.as_table()
            .unwrap()
            .get("message")
            .unwrap()
            .as_str()
            .unwrap();
        
        let arms = format!("\t\t\t{}::{} => \"{}\",\n", enum_name, x, message);
        f.write(arms.as_bytes()).unwrap();
    });
    f.write(concat!("\t\t}\n").as_bytes())?;
    f.write(concat!("\t}\n").as_bytes())?;

    let ks = t.keys();

    f.write("\tfn name(&self) -> &'static str {\n\t\tmatch self {\n".as_bytes())?;
    ks.for_each(|x| {
        let v = t.get(x).unwrap();
        let message = v.as_table()
            .unwrap()
            .get("message")
            .unwrap()
            .as_str()
            .unwrap();

        let arms = format!("\t\t\t{}::{} => \"{}::{}\",\n", enum_name, x, enum_name, x);
        f.write(arms.as_bytes()).unwrap();
    });
    f.write(concat!("\t\t}\n").as_bytes())?;
    f.write(concat!("\t}\n").as_bytes())?;
    f.write(concat!("}\n").as_bytes())?;
    
    Ok(())
}

fn make_default_error(gen_f :&mut std::fs::File) -> Result<(), std::io::Error> {
    let data = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/asset/default.toml")).to_string();
    let meta = data.as_str().parse::<Table>().unwrap();
    let error_list = meta.get("error").unwrap();
    
    let root = error_list.as_table().unwrap();
    write_enum_ident(gen_f, "CommonDefaultErrorKind", root.keys())?;
    write_enum_trait(gen_f, "CommonDefaultErrorKind", &root)?;
    Ok(())
}

fn make_custom_error(gen_f : &mut std::fs::File) -> Result<(), std::io::Error> {
    let file = std::env::var("CARGO_FEATURE_FILE");
    if file.is_err() || file.as_ref().unwrap() == "" {
        return Ok(());
    }
    let file_path = file.unwrap();
    let f = std::fs::read(file_path.clone());
    let data = if f.is_err() {
        Err(std::io::Error::new(std::io::ErrorKind::NotFound, "File not found"))?
    } else {
        Ok::<String, std::io::Error>(String::from_utf8_lossy(f.unwrap().as_slice()).to_string())
    }?;
    let meta = data.as_str().parse::<Table>().unwrap();
    let error_list = meta.get("error").unwrap();
    
    let root = error_list.as_table().unwrap();
    write_enum_ident(gen_f, "CommonCustomErrorKind", root.keys())?;
    write_enum_trait(gen_f, "CommonCustomErrorKind", &root)?;
    Ok(())
}

fn main() -> Result<(), std::io::Error> {
    let gen_file = concat!(env!("CARGO_MANIFEST_DIR"), "/src/gen.rs");

    let mut gen_f = std::fs::OpenOptions::new().write(true).create(true).open(gen_file)?;
    gen_f.set_len(0)?;
    gen_f.write("use crate::CommonErrorKind;\n".as_bytes())?;
    
    make_default_error(&mut gen_f)?;
    make_custom_error(&mut gen_f)?;
    Ok(())
}