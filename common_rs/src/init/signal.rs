use std::sync::{Once, Mutex, LazyLock};
use std::collections::HashMap;



#[cfg(target_os = "linux")]
pub mod signum {
    pub use libc::SIGHUP;
    pub use libc::SIGINT;
    pub use libc::SIGQUIT;
    pub use libc::SIGILL;
    pub use libc::SIGABRT;
    pub use libc::SIGFPE;
    pub use libc::SIGSEGV;
    pub use libc::SIGPIPE;
    pub use libc::SIGTERM;
    pub use libc::SIGUSR1;
    pub use libc::SIGUSR2;
}

#[cfg(target_os = "windows")]
pub mod signum {
    pub use libc::SIGINT;
    pub use libc::SIGABRT;
    pub use libc::SIGFPE;
    pub use libc::SIGILL;
    pub use libc::SIGSEGV;
    pub use libc::SIGTERM;
}

#[cfg(target_os = "linux")]
pub(crate) const SIGNALS: &[libc::c_int] = &[
    libc::SIGHUP,
    libc::SIGINT,
    libc::SIGQUIT,
    libc::SIGILL,
    libc::SIGABRT,
    libc::SIGFPE,
    libc::SIGSEGV,
    libc::SIGPIPE,
    libc::SIGTERM,
    libc::SIGUSR1,
    libc::SIGUSR2,
];

#[cfg(target_os = "windows")]
pub(crate) const SIGNALS: &[libc::c_int] = &[
    libc::SIGINT,
    libc::SIGABRT,
    libc::SIGFPE,
    libc::SIGILL,
    libc::SIGSEGV,
    libc::SIGTERM,
];




static SIGNAL_ONCE : Once = Once::new();
static SIGNAL_MAP : LazyLock<Mutex<HashMap<i32, bool>>> = LazyLock::new(|| {
    Mutex::new(HashMap::<i32, bool>::new())
});

extern "C" fn signal_handle(num : libc::c_int) {
    let mut map = SIGNAL_MAP.lock().unwrap();

    let idx : i32 = num;
    map.entry(idx).or_insert(true);
}

pub fn init_once() {
    SIGNAL_ONCE.call_once(|| {
        unsafe {
            let mut map = SIGNAL_MAP.lock().unwrap();

            for sig in SIGNALS {
                map.insert(*sig, true);
                libc::signal(*sig, signal_handle as usize);
            }
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