use std::{ffi, fs, path::Path, slice, sync::Mutex, time::Duration};

use super::pmix::{globals, sys};

// TODO: Proper network communication
pub struct FileFence<'a> {
    dir: &'a Path,
    nnodes: u32,
    rank: u32,
    callback: Mutex<Option<globals::ModexCallback>>,
}

impl<'a> FileFence<'a> {
    pub fn new(dir: &'a Path, nnodes: u32, rank: u32) -> Self {
        fs::create_dir_all(dir.join("fence/data")).unwrap();
        fs::create_dir_all(dir.join("fence/done")).unwrap();
        Self {
            dir,
            nnodes,
            rank,
            callback: Mutex::new(None),
        }
    }

    fn list_files(&self, dir: &Path) -> Vec<fs::DirEntry> {
        fs::read_dir(dir)
            .unwrap()
            .map(|r| r.unwrap())
            .collect::<Vec<_>>()
    }

    fn wait_for_no_files(&self, dir: &Path) {
        loop {
            let files = self.list_files(dir);
            if files.len() == 0 {
                break;
            } else {
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }

    fn collect_data(&self, dir: &Path) -> Option<Vec<u8>> {
        let files = self.list_files(dir);
        if files.len() != self.nnodes as usize {
            return None;
        }

        let data = files.iter().fold(Vec::new(), |mut acc, p| {
            let d = fs::read(p.path()).unwrap();
            acc.extend(d);
            acc
        });

        Some(data)
    }

    pub fn submit(
        &self,
        procs: Vec<sys::pmix_proc_t>,
        data: globals::CData,
        cb: globals::ModexCallback,
    ) {
        // TODO: Handle other fence scopes
        assert_eq!(procs.len(), 1);
        assert_eq!(procs[0].rank, sys::PMIX_RANK_WILDCARD);

        // FIXME: We are responsible for freeing data.
        let data = if !data.0.is_null() {
            unsafe { slice::from_raw_parts(data.0, data.1) }
        } else {
            &[]
        };

        let rank = self.rank.to_string();

        let data_path = self.dir.join("fence/data").join(&rank);
        fs::write(&data_path, data).unwrap();

        {
            let mut guard = self.callback.lock().unwrap();
            assert_eq!(*guard, None);
            *guard = Some(cb);
        }
    }

    pub fn handle_files(&self) {
        let rank = self.rank.to_string();
        let data_prefix = self.dir.join("fence/data");
        let done_prefix = self.dir.join("fence/done");
        let Some(data) = self.collect_data(&data_prefix) else {
            return;
        };

        let done_path = done_prefix.join(&rank);
        fs::write(&done_path, "").unwrap();
        while self.list_files(&done_prefix).len() != self.nnodes as usize {
            std::thread::sleep(Duration::from_millis(100));
        }

        fs::remove_file(data_prefix.join(rank)).unwrap();
        self.wait_for_no_files(&data_prefix);
        fs::remove_file(done_path).unwrap();
        self.wait_for_no_files(&done_prefix);

        let cb = {
            let mut guard = self.callback.lock().unwrap();
            guard.take().unwrap()
        };

        let (Some(cbfunc), cbdata) = cb else { return };

        let data = Box::new(data);
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
