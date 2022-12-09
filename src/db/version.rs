use std::fs::File;

// all sstable meta
struct Version {
    //
    levels: Vec<Level>,
    // levels:
}

struct Level {
    // if j>i, all key in file j > file i
    sorted_sstable_files: Vec<File>,
}
