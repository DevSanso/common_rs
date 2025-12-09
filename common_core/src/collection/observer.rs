use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
pub struct Subject<T : 'static + Clone> {
    current : Option<T>,
    seq     : AtomicU64
}

impl<T : 'static + Clone> Subject<T> {
    pub fn new() -> Subject<T> {
        Subject { current : None, seq: AtomicU64::new(0) }
    }

    pub fn new_arc() -> Arc<Subject<T>> {
        Arc::new(Subject { current : None, seq: AtomicU64::new(0) })
    }

    pub fn notify(&mut self, val : T) {
        self.current.replace(val);
        self.seq.fetch_add(1, Ordering::SeqCst);
    }
}

pub struct Observer<'a, T : 'static + Clone> {
    observer : &'a Subject<T>,
    seq     : AtomicU64
}

impl<'a, T : 'static + Clone> Observer<'a, T> {
    pub fn subscribe(subject : &'_ Subject<T>) -> Observer<'_, T> {
        Observer {
            observer : &subject,
            seq      : AtomicU64::new(subject.seq.load(Ordering::SeqCst)),
        }
    }

    pub fn update(&self) -> Option<T> {
        if self.observer.seq.load(Ordering::SeqCst) == self.seq.load(Ordering::Relaxed) {
           None
        } else {
            let ret = self.observer.current.as_ref().cloned();
            self.seq.store(self.observer.seq.load(Ordering::SeqCst), Ordering::Relaxed);
            ret
        }
    }
}

pub struct ThreadSafeObserver<T : 'static + Clone> {
    observer : Arc<Subject<T>>,
    seq     : AtomicU64
}

impl<T : 'static + Clone> ThreadSafeObserver<T> {
    pub fn subscribe(subject : Arc<Subject<T>>) -> ThreadSafeObserver<T> {
        let seq = subject.seq.load(Ordering::SeqCst);
        ThreadSafeObserver {
            observer : subject,
            seq      : AtomicU64::new(seq),
        }
    }

    pub fn update(&self) -> Option<T> {
        if self.observer.seq.load(Ordering::SeqCst) == self.seq.load(Ordering::Relaxed) {
            None
        } else {
            let ret = self.observer.current.as_ref().cloned();
            self.seq.store(self.observer.seq.load(Ordering::SeqCst), Ordering::Relaxed);
            ret
        }
    }
}

