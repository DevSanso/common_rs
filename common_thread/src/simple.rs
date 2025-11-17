use std::error::Error;
use std::sync::Arc;
use crate::simple::thread_pool::ThreadPool;
use common_err::CommonError;
mod instant_manager;
mod thread_pool;

type ThreadFn<T> = dyn Fn(T) + Send + Sync;

pub trait SimpleThreadManager<T : 'static + Send> {
    fn execute(&self, name : String, f :  &'static ThreadFn<T>, arg : T) -> Result<(), CommonError>;
}

pub enum SimpleManagerKind {
    Instant,
    Pool
}

pub fn new_simple_thread_manager<T: 'static + Send>(kind : SimpleManagerKind, max : usize)
    -> Arc<dyn SimpleThreadManager<T>> {
    match kind {
        SimpleManagerKind::Instant => Arc::new(instant_manager::InstantThreadManager::new(max)),
        SimpleManagerKind::Pool => Arc::new(ThreadPool::new(max)),
    }
}