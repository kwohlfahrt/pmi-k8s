use std::{
    ffi,
    marker::PhantomData,
    sync::{RwLock, mpsc},
};
use thiserror::Error;

use super::sys;

pub enum Event {
    Fence {
        data: (*mut ffi::c_char, usize),
        cb: (sys::pmix_modex_cbfunc_t, *mut ffi::c_void),
    },
}

unsafe impl Send for Event {}

pub enum State {
    Client,
    Server(mpsc::Sender<Event>),
}

pub static PMIX_STATE: RwLock<Option<State>> = RwLock::new(None);

#[derive(Error, Debug)]
#[error("PMIx was already initialized")]
pub struct AlreadyInitialized();

pub struct Unsync(pub PhantomData<*const ()>);
unsafe impl Send for Unsync {}
