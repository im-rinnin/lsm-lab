pub mod simple_rand {
    const A: u64 = 47;
    const C: u64 = 113;
    const M: u64 = i32::MAX as u64 - 1;

    struct Rand {
        next: u64,
    }

    impl Rand {
        pub fn new() -> Rand {
            Rand::with_seed(42)
        }
        pub fn with_seed(seed: u64) -> Rand {
            let mut res = Rand { next: seed };
            res.next();
            res
        }
        pub fn next(&mut self) -> u64 {
            let res = self.next;
            self.next = (res * A + C) % M;
            res
        }
    }

    #[cfg(test)]
    mod test {
        #[test]
        fn test() {
            let mut r = super::Rand::new();
            assert_eq!(r.next(), 2087);
            assert_eq!(r.next(), 98202);
            assert_eq!(r.next(), 4615607);
        }
    }
}