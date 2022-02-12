extern crate lru;

use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn fibonacci(n: u64) -> u64 {
    match n {
        0 => 1,
        1 => 1,
        n => fibonacci(n - 1) + fibonacci(n - 2),
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    std::thread::sleep_ms(100);
    c.bench_function("fib 21", |b| b.iter(|| lru::test(3)));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
