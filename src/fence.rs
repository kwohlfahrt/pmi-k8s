use std::{fs, path::Path, time::Duration};

// TODO: Proper network communication
pub struct FileFence<'a> {
    dir: &'a Path,
    nnodes: u32,
    rank: u32,
}

impl<'a> FileFence<'a> {
    pub fn new(dir: &'a Path, nnodes: u32, rank: u32) -> Self {
        Self { dir, nnodes, rank }
    }

    fn wait_for_no_files(&self, dir: &Path) {
        loop {
            let files = fs::read_dir(dir)
                .unwrap()
                .map(|r| r.unwrap())
                .collect::<Vec<_>>();
            if files.len() == 0 {
                break;
            } else {
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }

    fn wait_for_files(&self, dir: &Path) -> Vec<u8> {
        let files = loop {
            let files = fs::read_dir(dir)
                .unwrap()
                .map(|r| r.unwrap())
                .collect::<Vec<_>>();
            if files.len() == self.nnodes as usize {
                break files;
            } else {
                std::thread::sleep(Duration::from_millis(100));
            }
        };

        files.iter().fold(Vec::new(), |mut acc, p| {
            let d = fs::read(p.path()).unwrap();
            acc.extend(d);
            acc
        })
    }

    pub fn fence(&self, data: &[u8]) -> Box<Vec<u8>> {
        let rank = self.rank.to_string();

        let data_prefix = self.dir.join("fence/data");
        fs::create_dir_all(&data_prefix).unwrap();
        let done_prefix = self.dir.join("fence/done");
        fs::create_dir_all(&done_prefix).unwrap();

        let data_path = data_prefix.join(&rank);
        fs::write(&data_path, data).unwrap();
        let data = Box::new(self.wait_for_files(&data_prefix));

        let done_path = done_prefix.join(&rank);
        fs::write(&done_path, rank).unwrap();
        self.wait_for_files(&done_prefix);

        fs::remove_file(data_path).unwrap();
        self.wait_for_no_files(&data_prefix);
        fs::remove_file(done_path).unwrap();
        self.wait_for_no_files(&done_prefix);

        data
    }
}

#[cfg(test)]
mod test {
    use std::{collections::HashSet, thread};
    use tempdir::TempDir;

    use super::*;

    #[test]
    fn test_fence() {
        let nnodes = 3;
        let dir = TempDir::new("test-fence").unwrap();
        let results = thread::scope(|scope| {
            let ts = (0..nnodes)
                .map(|i| {
                    let dir = dir.path();
                    scope.spawn(move || {
                        let f = FileFence::new(dir, nnodes, i);
                        f.fence(&[i as u8])
                    })
                })
                .collect::<Vec<_>>();

            ts.into_iter()
                .map(|t| *t.join().unwrap())
                .collect::<Vec<_>>()
        });

        // Order is not guaranteed, so compare as set
        let expected = (0..nnodes as u8).collect::<HashSet<_>>();
        for result in results {
            let result = result.into_iter().collect::<HashSet<_>>();
            assert_eq!(result.len(), nnodes as usize);
            assert_eq!(result, expected);
        }
    }
}
