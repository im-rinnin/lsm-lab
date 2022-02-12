use std::collections::HashMap;

struct LruItem<K, V> {
    value: V,
    insert_after_key: Option<K>,
    insert_before_key: Option<K>,
}

impl<K, V> LruItem<K, V> {
    fn new(value: V) -> LruItem<K, V> {
        LruItem {
            value,
            insert_after_key: Option::None,
            insert_before_key: Option::None,
        }
    }
}

// insert to tail,pop from head
pub struct Lru<K: Hash + Eq + Clone, V> {
    map: HashMap<K, LruItem<K, V>>,
    head_key: Option<K>,
    tail_key: Option<K>,
    capacity: usize,
}

use std::borrow::Borrow;
use std::hash::Hash;

impl<K: Hash + Eq + Copy, V> Lru<K, V> {
    pub fn new(size: usize) -> Lru<K, V> {
        assert!(size > 0);
        Lru {
            capacity: size,
            head_key: Option::None,
            tail_key: Option::None,
            map: HashMap::with_capacity(size),
        }
    }

    pub fn get(&self, key: K) -> Option<&V> {
        let res = self.map.get(&key).map(|i| i.value.borrow());
        res
    }
    pub fn delete(&mut self, key: K) -> Option<V> {
        if let Some(mut item) = self.map.remove(&key) {
            self.remove_from_list(key, &mut item);
            return Some(item.value);
        }
        None
    }
    pub fn set(&mut self, key: K, value: V) {
        // replace
        if self.map.contains_key(&key) {
            let mut get_res = self.map.remove(&key).unwrap();
            get_res.value = value;

            // fix before and after item in db
            self.remove_from_list(key, &mut get_res);

            // set to None
            get_res.insert_after_key = Option::None;
            get_res.insert_before_key = Option::None;

            self.add_to_tail(key, &mut get_res);
            self.map.insert(key, get_res);

            return;
        }
        // evict
        if self.capacity == self.map.len() {
            let pop_key = self.head_key.as_ref().unwrap();
            let pop_item = self.map.remove(pop_key).unwrap();
            self.head_key = pop_item.insert_before_key;
        }

        // add to tail
        let mut item = LruItem::new(value);
        self.add_to_tail(key, &mut item);
        self.map.insert(key, item);
    }

    fn remove_from_list(&mut self, key: K, get_res: &mut LruItem<K, V>) {
        if let Some(insert_before_key) = get_res.insert_before_key {
            self.map
                .get_mut(&insert_before_key)
                .unwrap()
                .insert_after_key = get_res.insert_after_key;
        }
        if let Some(insert_after_key) = get_res.insert_after_key {
            self.map
                .get_mut(&insert_after_key)
                .unwrap()
                .insert_before_key = get_res.insert_before_key;
        }

        if self.head_key.is_some() {
            // both key is some
            // fix head
            if self.head_key.unwrap() == key {
                self.head_key = get_res.insert_after_key;
            }
            // fix tail
            if self.tail_key.unwrap() == key {
                self.tail_key = get_res.insert_before_key;
            }
        }
    }

    fn add_to_tail(&mut self, key: K, item: &mut LruItem<K, V>) {
        //     if is empty
        if self.tail_key.is_none() {
            assert!(self.head_key.is_none());
            self.tail_key = Option::Some(key);
            self.head_key = Option::Some(key);
        } else {
            self.map
                .get_mut(&self.tail_key.unwrap())
                .unwrap()
                .insert_after_key = Option::Some(key);
            self.tail_key = Option::Some(key);
            item.insert_before_key = self.tail_key;
        }
    }
}

#[cfg(test)]
mod test {
    use super::Lru;

    #[test]
    fn test_set_and_get() {
        let mut lru: Lru<i32, i32> = Lru::new(10);
        assert!(lru.get(10).is_none());
        lru.set(10, 10);
        assert_eq!(*lru.get(10).unwrap(), 10);
    }

    #[test]
    fn test_lru_evict() {
        let mut lru: Lru<i32, i32> = Lru::new(2);
        lru.set(1, 1);
        lru.set(2, 2);
        lru.set(3, 3);
        assert_eq!(*lru.get(2).unwrap(), 2);
        assert_eq!(*lru.get(3).unwrap(), 3);
        assert!(lru.get(1).is_none());
    }

    #[test]
    fn test_lru_overwrite() {
        let mut lru: Lru<i32, i32> = Lru::new(2);
        lru.set(1, 1);
        assert_eq!(*lru.get(1).unwrap(), 1);
        lru.set(1, 3);
        assert_eq!(*lru.get(1).unwrap(), 3);
    }

    #[test]
    fn test_lru_delete() {
        let mut lru: Lru<i32, i32> = Lru::new(3);
        assert!(lru.delete(3).is_none());
        lru.set(1, 2);
        lru.set(3, 4);
        assert_eq!(lru.delete(1).unwrap(), 2);
        assert!(lru.delete(1).is_none());
        assert_eq!(*lru.get(3).unwrap(), 4)
    }
}
