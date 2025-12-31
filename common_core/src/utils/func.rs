use rand::distr::Alphanumeric;
use rand::Rng;
use super::types::*;
use rustc_demangle::demangle;
use backtrace::Backtrace;
use backtrace::BacktraceFrame;

pub fn enum_option_get_only_one<E, T>(
    value: Option<E>,
) -> Result<T, String>
where
    E: IntoEnum<T>,
{
    match value {
        Some(e) => e.into_enum(),
        None => Err("enum:Unknown, sub:Unknown".to_string()), // 디폴트 메시지
    }
}

pub fn enum_get_only_one<E, T>(
    value: E,
) -> Result<T, String>
where
    E: IntoEnum<T>,
{
    value.into_enum()
}

pub fn enum_get_only_one_ref<E, T>(
    value: &E,
) -> Result<T, String>
where
    E: CloneEnum<T>,
{
    value.clone_enum()
}

pub fn generate_random_string(length: usize) -> String {
    let mut rng = rand::rng();

    (0..length)
        .map(|_| rng.sample(Alphanumeric))
        .map(char::from)
        .collect()
}

fn decode_bt_frames(frames : &[BacktraceFrame], idx : usize) -> String {
    if let Some(frame) = frames.get(idx) {
        for symbol in frame.symbols() {
            if let Some(name) = symbol.name() {
                let de = demangle(name.as_str().unwrap()).to_string();


                let parts: Vec<&str> = de.as_str().split("::").collect();

                if parts.len() >= 2 && parts.last().unwrap().starts_with('h') {
                    return parts.get(parts.len() - 2).unwrap().to_string();
                }

                return parts.last().unwrap().to_string();
            }
        }
    }

    String::from("")
}

pub fn get_current_func_name(idx : usize) -> String {
    let mut bt = Backtrace::new_unresolved();
    bt.resolve();

    decode_bt_frames(bt.frames(), idx)
}