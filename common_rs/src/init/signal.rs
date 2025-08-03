use std::sync::{Once, Mutex, LazyLock};
use std::collections::HashMap;


pub use libc::SIGINT;
pub use libc::SIGABRT;
pub use libc::SIGBUS;
pub use libc::SIGPIPE;

static SIGNAL_ONCE : Once = Once::new();
static SIGNAL_MAP : LazyLock<Mutex<HashMap<i32, bool>>> = LazyLock::new(|| {
    Mutex::new(HashMap::<i32, bool>::new())
});

extern "C" fn signal_handle(num : libc::c_int) {
    let mut map = SIGNAL_MAP.lock().unwrap();
    match num {
        SIGINT => *map.get_mut(&SIGINT).unwrap() = true,
        SIGABRT => *map.get_mut(&SIGABRT).unwrap() = true,
        SIGBUS => *map.get_mut(&SIGBUS).unwrap() = true,
        SIGPIPE => *map.get_mut(&SIGPIPE).unwrap() = true,
        _ => {}
    }
}

pub fn init_once() {
    SIGNAL_ONCE.call_once(|| {
        unsafe {
            let mut map = SIGNAL_MAP.lock().unwrap();
            map.insert(SIGINT, false);
            map.insert(SIGABRT, false);
            map.insert(SIGBUS, false);
            map.insert(SIGPIPE, false);

            libc::signal(SIGINT, signal_handle as usize);
            libc::signal(SIGABRT, signal_handle as usize);
            libc::signal(SIGBUS, signal_handle as usize);
            libc::signal(SIGPIPE, signal_handle as usize);
        }
    });
}

pub(crate) fn is_set_signal(signal : i32) -> bool {
    let map = SIGNAL_MAP.lock().unwrap();
    match map.get(&signal) {
        Some(s) => *s,
        None => false
    }
}