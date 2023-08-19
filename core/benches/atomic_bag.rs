use std::{
    sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
    thread,
    time::{Duration, Instant},
};

use criterion::{criterion_group, criterion_main, Criterion};

fn questions(c: &mut Criterion) {
    let mut group = c.benchmark_group("atomic_bag/questions");

    group.bench_function("swap", |b| {
        b.iter_custom(|iters| {
            let slot = AtomicU64::new(0);
            thread::scope(|scope| {
                scope.spawn(|| {
                    let mut items_sent = 1;
                    while items_sent <= iters {
                        if slot.swap(items_sent, Relaxed) == 0 {
                            items_sent += 1;
                        }
                    }
                });

                let consumer = scope.spawn(|| {
                    let time = Instant::now();

                    let mut items_seen = 0;
                    while items_seen < iters {
                        if slot.swap(0, Relaxed) != 0 {
                            items_seen += 1;
                        }
                    }

                    time.elapsed()
                });

                consumer.join().unwrap()
            })
        });
    });

    group.bench_function("load_store", |b| {
        b.iter_custom(|iters| {
            let flag = AtomicBool::new(false);
            let slot = AtomicU64::new(0);
            thread::scope(|scope| {
                scope.spawn(|| {
                    let mut items_sent = 0;
                    while items_sent < iters {
                        if !flag.load(Relaxed) {
                            slot.store(items_sent, Relaxed);
                            flag.store(true, Relaxed);
                            items_sent += 1;
                        }
                    }
                });

                let consumer = scope.spawn(|| {
                    let time = Instant::now();

                    let mut items_seen = 0;
                    while items_seen < iters {
                        if flag.load(Relaxed) {
                            std::hint::black_box(slot.load(Relaxed));
                            flag.store(false, Relaxed);
                            items_seen += 1;
                        }
                    }

                    time.elapsed()
                });

                consumer.join().unwrap()
            })
        });
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().noise_threshold(0.02).warm_up_time(Duration::from_secs(1));
    targets =
    questions,
}
criterion_main!(benches);
