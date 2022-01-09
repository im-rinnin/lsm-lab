#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]

mod debug {
    use std::fmt::Display;

    struct A<T> {
        a: T,
    }

    impl<T> Drop for A<T> {
        fn drop(&mut self) {
            println!("hi");
        }
    }

    impl<T: Display> A<T> {
        fn print(&self) -> &str {
            "dsf"
        }
    }

    #[cfg(test)]
    mod test {
        use super::A;
        use std::borrow::Borrow;
        use std::cell::RefCell;
        use std::ptr::slice_from_raw_parts_mut;
        use std::sync::atomic::{AtomicI8, AtomicPtr, Ordering};
        use std::sync::{Arc, Mutex};
        use std::thread::spawn;

        #[test]
        fn test() {
            // let lock = Arc::new(AtomicI8::new(1));
            //
            // let mut jv = vec![];
            // for i in 1..10 {
            //     let lock_c = lock.clone();
            //     let j = spawn(move || {
            //         let s = lock_c.fetch_add(1, Ordering::SeqCst);
            //         println!("{}", s);
            //     });
            //     jv.push(j);
            // }
            // for i in 1..10 {
            //     let j = jv.pop();
            //     j.unwrap().join();
            // }
        }
    }
}
