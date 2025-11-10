use std::error::Error;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{Ordering, AtomicUsize, AtomicBool};
use std::thread;

use common_core::logger;
use common_core::utils::types::SimpleError;

use crate::simple::ThreadFn;
use crate::simple::SimpleThreadManager;

struct ThreadPoolState {
    current : AtomicUsize
}

impl ThreadPoolState {
    pub fn new() -> Arc<Self> {
        Arc::new(ThreadPoolState {current: AtomicUsize::new(0) })
    }

    pub fn add_current(self : &Arc<Self>) {
        self.current.fetch_add(1, Ordering::SeqCst);
    }

    pub fn sub_current(self : &Arc<Self>) {
        self.current.fetch_sub(1, Ordering::SeqCst);
    }

    pub fn get_current(self : &Arc<Self>) -> usize {self.current.load(Ordering::SeqCst)}
}

pub(super) struct InstantThreadManager<T> where T : 'static + Send{
    thread_cnt_limit : usize,
    state : Arc<ThreadPoolState>,
    _mark : PhantomData<T>,
}

impl<T : 'static + Send> InstantThreadManager<T> {
    pub fn new(max: usize) -> InstantThreadManager<T> {
        InstantThreadManager {
            thread_cnt_limit : max,
            state : ThreadPoolState::new(),
            _mark:PhantomData
        }
    }

    pub fn get_sizes(&self) -> (usize, usize) {(self.thread_cnt_limit, self.state.get_current())}
}

impl <T : 'static + Send> SimpleThreadManager<T> for InstantThreadManager<T> {
    fn execute(&self, name : String, f :  &'static ThreadFn<T>, arg : T) -> Result<(), Box<dyn Error>> {
        if self.state.get_current() >= self.thread_cnt_limit {
            return SimpleError{msg : format!("ThreadPool - execute - limit {}/{}"
                                             , self.state.get_current(), self.thread_cnt_limit)}.to_result();
        }

        let clone_state = self.state.clone();

        thread::Builder::new()
            .stack_size(4 * 1024 * 1024)
            .name(name.clone())
            .spawn(move || {
                clone_state.add_current();
                logger::debug!( "Thread pool started with {} threads", name );
                f(arg);
                clone_state.sub_current();
                logger::debug!( "Thread pool ended {}", name );
            }).map_err(|e|{
            SimpleError {msg : format!("ThreadPool - spawn - {}", e.to_string())}
        })?;

        Ok(())
    }
}