use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
pub struct ChangeSubject<T : 'static + Clone> {
    current : Option<T>,
    seq     : AtomicU64
}

impl<T : 'static + Clone> ChangeSubject<T> {
    pub fn new() -> ChangeSubject<T> {
        ChangeSubject { current : None, seq: AtomicU64::new(0) }
    }

    pub fn new_arc() -> Arc<ChangeSubject<T>> {
        Arc::new(ChangeSubject { current : None, seq: AtomicU64::new(0) })
    }

    pub fn notify(&mut self, val : T) {
        self.current.replace(val);
        self.seq.fetch_add(1, Ordering::SeqCst);
    }
}

pub struct ChangeObserver<'a, T : 'static + Clone> {
    observer : &'a ChangeSubject<T>,
    seq     : AtomicU64
}

impl<'a, T : 'static + Clone> ChangeObserver<'a, T> {
    pub fn subscribe(subject : &'_ ChangeSubject<T>) -> ChangeObserver<'_, T> {
        ChangeObserver {
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

pub struct ChangeThreadSafeObserver<T : 'static + Clone> {
    observer : Arc<ChangeSubject<T>>,
    seq     : AtomicU64
}

impl<T : 'static + Clone> ChangeThreadSafeObserver<T> {
    pub fn subscribe(subject : Arc<ChangeSubject<T>>) -> ChangeThreadSafeObserver<T> {
        let seq = subject.seq.load(Ordering::SeqCst);
        ChangeThreadSafeObserver {
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

