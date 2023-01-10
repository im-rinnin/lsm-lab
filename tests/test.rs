use lsm_db::{
    self,
    db::{config::Config, key::Key, value::Value, DBClient, DBServer},
};
use std::{
    collections::HashSet,
    fs::{create_dir, remove_dir_all},
    path::PathBuf,
    sync::{atomic::AtomicU64, Arc, Mutex},
    thread::spawn,
};
use tempfile::tempdir;

fn random_number(num: u64) -> u64 {
    (num * 1103515245 + 12345) % (2 << 31)
}

// use mutiple thread to  write to db and check some sample data
#[test]
fn full_test() {
    // let r = init_test_log_as_info_and_metric();
    let check_id_number = 100;
    let write_thread_number = 10;
    let round = 100000;

    // key used in write/check routine is create by random(global_id)
    let global_id = Arc::new(AtomicU64::new(1));
    // create db
    let dir = tempdir().unwrap();

    remove_dir_all(&dir).unwrap();
    create_dir(dir.path()).unwrap();
    let db = DBServer::new(PathBuf::from(dir.path())).unwrap();
    // check map check all key=random_number(id) and id <100
    let mut lock_map = Vec::new();
    for _ in 0..check_id_number {
        lock_map.push(Arc::new(Mutex::new(None)));
    }
    let mut handls = Vec::new();
    let mut check_ids = HashSet::new();
    for i in 0..check_id_number {
        check_ids.insert(random_number(i));
    }
    for i in 0..write_thread_number {
        let id_clone = global_id.clone();
        let client = db.new_client().unwrap();
        let lock_map_clone = lock_map.clone();
        let check_ids_clone = check_ids.clone();
        let j = spawn(move || {
            write_routine(round, i, id_clone, client, check_ids_clone, lock_map_clone)
        });
        handls.push(j);
    }
    let client = db.new_client().unwrap();
    let lock_map_clone = lock_map.clone();
    let j = spawn(move || check_routine(round, 10, client, lock_map_clone));
    handls.push(j);

    for j in handls {
        j.join().unwrap();
    }

    db.close().unwrap();
    let db = DBServer::open_db(PathBuf::from(dir.path()), Config::new()).unwrap();
    let db_client = db.new_client().unwrap();
    check_routine(round, 10, db_client, lock_map);
}
fn write_routine(
    round: u64,
    mut rand: u64,
    global_id: Arc<AtomicU64>,
    mut db_client: DBClient,
    check_set: HashSet<u64>,
    lock_map: Vec<Arc<Mutex<Option<(u64, Option<u64>)>>>>,
) {
    for _ in 0..round {
        let mut lock_option = None;
        rand = random_number(rand);
        let mut is_checked_id = false;
        let id = if rand % 50 == 0 {
            let id = rand % 100;
            let lock = &lock_map[id as usize];
            lock_option = Some(lock.lock().unwrap());
            is_checked_id = true;
            assert!(id < 100);
            id
        } else {
            let id = global_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 100;
            assert!(id > 100);
            id
        };
        let value_id = rand;
        let value = Value::from_u64(value_id);
        let op = rand % 10;
        // set new kv
        if op < 7 {
            let key_id = random_number(id);
            if !is_checked_id && check_set.contains(&key_id) {
                continue;
            }
            let key = Key::from_u64(key_id);
            db_client.put(&key, value).unwrap();

            if let Some(lock) = &mut lock_option {
                **lock = Some((key_id, Some(value_id)));
            }
        }
        // override new kv;
        else if op == 7 {
            let mut key_id = random_number(rand % id);
            if !is_checked_id && check_set.contains(&key_id) {
                continue;
            }
            if is_checked_id {
                key_id = random_number(id);
            }
            let key = Key::from_u64(key_id);
            db_client.put(&key, value).unwrap();
            if let Some(lock) = &mut lock_option {
                **lock = Some((key_id, Some(value_id)));
            }
        }
        // delete exits key;
        else if op == 8 {
            let mut key_id = random_number(rand % id);
            if !is_checked_id && check_set.contains(&key_id) {
                continue;
            }
            if is_checked_id {
                key_id = random_number(id);
            }
            let key = Key::from_u64(key_id);
            db_client.delete(&key).unwrap();
            if let Some(lock) = &mut lock_option {
                **lock = Some((key_id, None));
            }
        }
        // delete not exit key;
        else {
            let key_id = random_number(id + 10000000);
            if !is_checked_id && check_set.contains(&key_id) {
                continue;
            }
            let key = Key::from_u64(key_id);
            db_client.delete(&key).unwrap();
        }
    }
}
fn check_routine(
    round: u64,
    mut rand: u64,
    db_client: DBClient,
    lock_map: Vec<Arc<Mutex<Option<(u64, Option<u64>)>>>>,
) {
    for _ in 0..round {
        rand = random_number(rand);
        let kv = lock_map
            .get(rand as usize % lock_map.len())
            .unwrap()
            .lock()
            .unwrap();

        if let Some((key_id, value_id)) = kv.as_ref() {
            let res = db_client.get(&Key::from_u64(*key_id)).unwrap();
            let expect = value_id.map(|n| Value::from_u64(n));
            assert_eq!(res, expect, "key not match {}", key_id);
        }
    }
}
