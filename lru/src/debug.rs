#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]

mod debug {
    use std::fmt::Display;

    #[derive(Copy)]
    struct B<'a> {
        a: &'a i32,
    }

    impl<'a> Clone for B<'a> {
        fn clone(&self) -> Self {
            todo!()
            // B { a: self.a }
        }
    }

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
        use crate::debug::debug::B;
        use std::borrow::Borrow;
        use std::cell::RefCell;
        use std::ptr::slice_from_raw_parts_mut;
        use std::sync::atomic::{AtomicI8, AtomicPtr, Ordering};
        use std::sync::{Arc, Mutex};
        use std::thread::spawn;

        #[test]
        fn test() {
            let m = 3;
            let a = B { a: &m };
            let c = a;
            println!("{}", *c.a);
            use rand::seq::SliceRandom;
            use rand::thread_rng;

            let mut rng = thread_rng();
            let mut y = vec![2, 4, 5, 6, 7];
            println!("Unshuffled: {:?}", y);
            y.shuffle(&mut rng);
            println!("Shuffled:   {:?}", y);
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
