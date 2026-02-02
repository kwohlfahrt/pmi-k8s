use std::{
    collections::HashMap,
    ffi::{self, CStr},
    fs,
    path::Path,
    slice,
    sync::Mutex,
};

use crate::pmix::globals::ModexCallback;

use super::pmix::{globals, sys};

unsafe extern "C" fn response(
    status: sys::pmix_status_t,
    data: *mut std::ffi::c_char,
    sz: usize,
    cbdata: *mut std::ffi::c_void,
) {
    assert_eq!(status, sys::PMIX_SUCCESS as sys::pmix_status_t);
    let data = if !data.is_null() {
        unsafe { slice::from_raw_parts(data, sz) }
    } else {
        &[]
    };
    let data = data.to_vec();
    let (node_rank, proc) = *unsafe { Box::from_raw(cbdata as *mut (u32, sys::pmix_proc_t)) };

    let guard = globals::PMIX_STATE.read().unwrap();
    if let Some(globals::State::Server(ref s)) = *guard {
        s.send(globals::Event::DirectModexResponse {
            node_rank,
            proc,
            data,
        })
        .unwrap();
    }
}

pub struct FileModex<'a> {
    dir: &'a Path,
    node_rank: u32,
    nproc: u16,
    callbacks: Mutex<HashMap<(String, u32), ModexCallback>>,
}

impl<'a> FileModex<'a> {
    pub fn new(dir: &'a Path, node_rank: u32, nproc: u16) -> Self {
        fs::create_dir_all(dir.join("modex/req").join(node_rank.to_string())).unwrap();
        fs::create_dir_all(dir.join("modex/resp").join(node_rank.to_string())).unwrap();

        Self {
            dir,
            node_rank,
            nproc,
            callbacks: Mutex::new(HashMap::new()),
        }
    }

    pub fn request(&self, proc: sys::pmix_proc_t, cb: globals::ModexCallback) {
        assert!(proc.rank <= sys::PMIX_RANK_VALID);
        let ns = ffi::CStr::from_bytes_until_nul(&proc.nspace)
            .unwrap()
            .to_str()
            .unwrap();
        let node_rank = proc.rank / self.nproc as u32;

        {
            let mut guard = self.callbacks.lock().unwrap();
            guard.insert((ns.to_string(), proc.rank), cb);
        }

        let req_path = self
            .dir
            .join("modex/req")
            .join(node_rank.to_string())
            .join(format!("{}-{}-{}", self.node_rank, ns, proc.rank));
        fs::write(req_path, "").unwrap();
    }

    fn parse_proc_filename(data: &str) -> (u32, sys::pmix_proc_t) {
        let mut splits = data.split(|c| c == '-');
        let node_rank = splits.next().unwrap().parse().unwrap();
        let nspace = splits.next().unwrap();
        let rank = splits.next().unwrap().parse().unwrap();
        let mut proc = sys::pmix_proc_t {
            nspace: [0; _],
            rank,
        };
        proc.nspace[0..nspace.len()].copy_from_slice(nspace.as_bytes());
        (node_rank, proc)
    }

    pub fn handle_files(&self) {
        let req_path = self.dir.join("modex/req").join(self.node_rank.to_string());
        for r in fs::read_dir(req_path)
            .unwrap()
            .into_iter()
            .map(|r| r.unwrap())
        {
            let (node_rank, proc) =
                Self::parse_proc_filename(&r.file_name().into_string().unwrap());
            let dst = Box::new((node_rank, proc));

            let status = unsafe {
                sys::PMIx_server_dmodex_request(
                    &proc,
                    Some(response),
                    Box::into_raw(dst) as *mut ffi::c_void,
                )
            };
            assert_eq!(status, sys::PMIX_SUCCESS as sys::pmix_status_t);
            fs::remove_file(r.path()).unwrap();
        }

        let resp_path = self.dir.join("modex/resp").join(self.node_rank.to_string());
        for r in fs::read_dir(resp_path)
            .unwrap()
            .into_iter()
            .map(|r| r.unwrap())
        {
            let (_, proc) = Self::parse_proc_filename(&r.file_name().into_string().unwrap());
            let data = Box::new(fs::read(r.path()).unwrap());
            fs::remove_file(r.path()).unwrap();

            let ns = ffi::CStr::from_bytes_until_nul(&proc.nspace)
                .unwrap()
                .to_str()
                .unwrap();

            let cb = {
                let mut guard = self.callbacks.lock().unwrap();
                guard.remove(&(ns.to_string(), proc.rank)).unwrap()
            };
            let (Some(cbfunc), cbdata) = cb else {
                continue;
            };

            unsafe {
                cbfunc(
                    sys::PMIX_SUCCESS as sys::pmix_status_t,
                    data.as_ptr(),
                    data.len(),
                    cbdata,
                    Some(globals::release_vec_u8),
                    Box::into_raw(data) as *mut ffi::c_void,
                )
            }
        }
    }

    pub fn handle_response(&self, node_rank: u32, proc: sys::pmix_proc_t, data: Vec<u8>) {
        let nspace = CStr::from_bytes_until_nul(&proc.nspace)
            .unwrap()
            .to_str()
            .unwrap();
        let resp_path = self
            .dir
            .join("modex/resp")
            .join(node_rank.to_string())
            .join(format!("{}-{}-{}", self.node_rank, nspace, proc.rank));
        fs::write(resp_path, data).unwrap();
    }
}
