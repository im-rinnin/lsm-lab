use crate::db::sstable::SSTable;

pub struct LevelInfo{
    sstable:Vec<SSTable>
}

pub struct LevelInfos{
    levels:Vec<LevelInfo>
}