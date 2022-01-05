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
        use std::thread::spawn;
        use std::sync::{Arc, Mutex};
        use std::cell::RefCell;
        use std::borrow::Borrow;

        #[test]
        fn test() {
            let a = Box::new(A { a: 3 });
            let b = Box::into_raw(a);
            unsafe {Box::from_raw(b);}
        }
    }
}