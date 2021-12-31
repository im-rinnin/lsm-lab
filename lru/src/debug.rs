#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]
mod debug {
    use std::fmt::Display;

    struct A<T> {
        a: T,
    }

    impl<T> A<T> {
        fn foo() -> usize {
            3
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

        #[test]
        fn test() {
            let a = A { a: 3 };
            a.print();
        }
    }
}