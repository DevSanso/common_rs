use std::marker::PhantomData;
use std::thread;
use std::sync::{mpsc, Arc, RwLock};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{Sender, Receiver};
use common_err::CommonError;
use common_err::gen::CommonDefaultErrorKind;
use crate::simple::ThreadFn;
use crate::simple::SimpleThreadManager;

#[derive(Clone, Eq, PartialEq, Debug)]
enum ThreadState {
    Running,
    Down,
    Idle,
}
struct ThreadStateMap {
    m: RwLock<Vec<ThreadState>>
}

struct ThreadChannelData<T : 'static + Send> {
    func : &'static ThreadFn<T>,
    data : T
}

impl ThreadStateMap {
    pub fn new(size : usize) -> Arc<Self> {
        let v = vec![ThreadState::Down; size];
        Arc::new(ThreadStateMap{ m: RwLock::new(v)})
    }

    pub fn get(self : &Arc<Self>, idx : usize) -> Result<ThreadState, CommonError> {
        let reader = self.m.read().map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::SystemCallFail, format!("ThreadStateMap - read - {}", e.to_string()))
        })?;

        Ok(reader[idx].clone())
    }

    pub fn set(self : &Arc<Self>, idx : usize, value : ThreadState) -> Result<(), CommonError> {
        let mut writer = self.m.write().map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::SystemCallFail, format!("ThreadStateMap - write - {}", e.to_string()))
        })?;
        writer[idx] = value;

        Ok(())
    }
}
pub(super) struct ThreadPool<T> where T : 'static + Send  {
    count : usize,
    state_map: Arc<ThreadStateMap>,
    senders : Vec<Sender<ThreadChannelData<T>>>,
    force_stop : Arc<AtomicBool>,
    _mark : PhantomData<T>,

    current_th_id : AtomicUsize
}

impl<T : 'static + Send> ThreadPool<T> {
    fn thread_entry(id : usize, state :Arc<ThreadStateMap>, force_stop : Arc<AtomicBool>, recv: Receiver<ThreadChannelData<T>>) {
        loop {
            if force_stop.load(Ordering::SeqCst) {
                break;
            }

            let recv_ret = recv.recv();
            if recv_ret.is_err() {
                break;
            }

            if state.set(id, ThreadState::Running).is_err() {
                break;
            }

            let rx_data = recv_ret.unwrap();

            let rx_func = rx_data.func;
            rx_func(rx_data.data);

            if state.set(id, ThreadState::Idle).is_err() {
                break;
            }
        }
    }

    fn create_threads(size : usize, state_map : &Arc<ThreadStateMap>) -> (Vec<Sender<ThreadChannelData<T>>>, Arc<AtomicBool>) {
        let mut v = Vec::with_capacity(size);
        let force_stop = Arc::new(AtomicBool::new(false));

        for i in 0..size {
            let (sender, receiver) = mpsc::channel::<ThreadChannelData<T>>();
            let clone_map = state_map.clone();
            let clone_force_stop  = force_stop.clone();
            let idx = i;

            v.push(sender);

            let _ = thread::Builder::new()
                .name(format!("threadPool-{}", i))
                .stack_size(4 * 1024 * 1024)
                .spawn(move || {
                    let down_chk_map = clone_map.clone();
                    let _ = down_chk_map.set(idx, ThreadState::Idle);
                    Self::thread_entry(idx, clone_map, clone_force_stop, receiver);
                    let _ = down_chk_map.set(idx, ThreadState::Down);
                });
        }

        (v, force_stop)
    }
    pub fn new(size : usize) -> Self {
        let state_map = ThreadStateMap::new(size);
        let (senders, force_stop_flag) = Self::create_threads(size, &state_map);

        ThreadPool {
            count : size,
            state_map,
            senders,
            force_stop: force_stop_flag,
            _mark: PhantomData,
            current_th_id : AtomicUsize::new(0)
        }
    }
}

impl<T : 'static + Send> Drop for ThreadPool<T> {
    fn drop(&mut self) {
        self.force_stop.store(true, Ordering::SeqCst);
    }
}

impl <T : 'static + Send> SimpleThreadManager<T> for ThreadPool<T> {
    fn execute(&self, _ : String, f :  &'static ThreadFn<T>, arg : T) -> Result<(), CommonError> {
        let data = ThreadChannelData {
            func : f,
            data : arg
        };

        let mut idx = self.current_th_id.fetch_add(1, Ordering::SeqCst);
        if idx >= self.count {
            idx = 0;
            self.current_th_id.store(0, Ordering::SeqCst);
        }

        self.senders[idx].send(data).map_err(|e| {
            CommonError::new(&CommonDefaultErrorKind::SystemCallFail, format!("ThreadPool - Execute - {}", e.to_string()))
        })?;

        Ok(())
    }
}