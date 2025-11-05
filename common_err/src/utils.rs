use rustc_demangle::demangle;
use backtrace::Backtrace;
use backtrace::BacktraceFrame;
use std::panic::Location;

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

    return String::from("")
}

pub(crate) fn get_source_func_name(idx : usize) -> String {
    let mut bt = Backtrace::new_unresolved();
    bt.resolve();

    decode_bt_frames(bt.frames(), idx)
}