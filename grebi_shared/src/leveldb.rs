use std::ffi::{CStr, c_void, CString};

#[repr(C)]
pub struct LevelDbIter {
    k: *const u8,
    v: *const u8
}


#[link(name = "grebi_leveldb", kind = "static" )]
#[link(name = "leveldb")]
extern "C" {

    pub fn grebi_leveldb_open(path:*const i8) -> *mut c_void;
    pub fn grebi_leveldb_close(db:*mut c_void);

    pub fn grebi_leveldb_put(db:*mut c_void, k:*mut c_void, klen:usize, v:*mut c_void, vlen:usize);
    pub fn grebi_leveldb_get(db:*mut c_void, k:*mut c_void, klen:usize) -> *mut c_void;

    pub fn grebi_leveldb_iter_start(db:*mut c_void) -> LevelDbIter;
    pub fn grebi_leveldb_iter_next(db:*mut c_void) -> LevelDbIter;
    pub fn grebi_leveldb_iter_end(db:*mut c_void);
}


pub struct LevelDb {
    db: *mut c_void
}

impl LevelDb {

    pub fn open(path:&str) -> LevelDb {
        let pathp = CString::new(path.as_bytes()).unwrap();
        unsafe {
        return LevelDb { db: grebi_leveldb_open(pathp.as_ptr()) };
        }
    }

    pub fn close(&self) {
        unsafe {
        grebi_leveldb_close(self.db);
        }
    }

    pub fn put(&self, k:&[u8], v:&[u8]) {
        unsafe {
            grebi_leveldb_put(
                self.db,
                k.as_ptr() as *mut c_void,
                k.len(),
                v.as_ptr() as *mut c_void,
                v.len());
        }
    }

    pub fn get(&self, k:&[u8]) -> Option<&[u8]> {
        unsafe {
            let ret_ptr = grebi_leveldb_get(self.db, k.as_ptr() as *mut c_void, k.len()) as *const i8;

            if ret_ptr.is_null() {
                return None;
            }

            return Some(CStr::from_ptr(ret_ptr).to_bytes());
        }
    }

}



