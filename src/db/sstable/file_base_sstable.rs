// use std::cell::RefCell;
// use std::fs::File;
// use std::sync::Arc;
//
// use crate::db::file_storage::{FileId, FileStorageManager};
// use crate::db::sstable::{SSTable, SStableMeta};
// use anyhow::Result;
//
//
// // for share sstable
// pub struct FileBaseSSTable {
//     file_id: FileId,
//     file_manager: FileStorageManager,
//     sstable_meta: Arc<SStableMeta>,
// }
//
// impl FileBaseSSTable {
//     pub fn new(sstable_meta: SStableMeta, file_id: FileId, file_manager: FileStorageManager) -> Self {
//         todo!()
//         // FileBaseSSTable { file_id, sstable_meta: Arc::new(sstable_meta), file_manager }
//     }
//     pub fn new_sstable(&mut self) -> Result<SSTable> {
//         let file = Box::new(RefCell::new(self.file_manager.open_file(self.file_id)?));
//         let sstable_meta = self.sstable_meta.clone();
//         SSTable::from(sstable_meta, file)
//     }
// }
//
//
// impl Clone for FileBaseSSTable {
//     fn clone(&self) -> Self {
//         todo!()
//     }
// }
//
