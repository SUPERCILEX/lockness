use std::{
    ptr::NonNull,
    sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
    thread,
    time::{Duration, Instant},
};

use criterion::{
    criterion_group, criterion_main, measurement::WallTime, BenchmarkGroup, Criterion,
};
use lockness_core::atomic_bag::{slot::mpsc, Allocated};

struct Invalid;

impl Allocated for Invalid {
    fn into_ptr(self) -> NonNull<()> {
        NonNull::dangling()
    }

    unsafe fn from_ptr(ptr: NonNull<()>) -> Self {
        std::hint::black_box(ptr);
        Invalid
    }
}

fn questions(c: &mut Criterion) {
    let mut group = c.benchmark_group("questions");

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

fn single_threaded(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_threaded");

    group.bench_function("atomic_bag/send", |b| {
        let (sender, _) = mpsc();
        b.iter(|| sender.try_send(Invalid));
    });

    group.bench_function("atomic_bag/receive", |b| {
        let (_, receiver) = mpsc::<Invalid>();
        b.iter(|| receiver.try_recv());
    });

    group.bench_function("atomic_bag/send_receive", |b| {
        let (sender, receiver) = mpsc();
        b.iter(|| {
            assert!(sender.try_send(Invalid).is_none());
            receiver.try_recv()
        });
    });

    group.bench_function("std/send", |b| {
        let (sender, _) = std::sync::mpsc::sync_channel(1);
        b.iter(|| sender.send(Invalid));
    });

    group.bench_function("std/receive", |b| {
        let (_, receiver) = std::sync::mpsc::sync_channel::<Invalid>(1);
        b.iter(|| receiver.try_recv());
    });

    group.bench_function("std/send_receive", |b| {
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        b.iter(|| {
            sender.send(Invalid).unwrap();
            receiver.try_recv()
        });
    });
}

fn multi_threaded(c: &mut Criterion) {
    let mut threads = 2;
    let available = thread::available_parallelism().unwrap().get();

    while threads <= available {
        let mut group = c.benchmark_group(format!("{threads}_threads"));

        producer_consumer(&mut group, threads - 1);
        threads *= 2;
    }

    {
        let mut group = c.benchmark_group("3_threads");
        producer_consumer(&mut group, 2);
    }
}

fn producer_consumer(group: &mut BenchmarkGroup<WallTime>, num_producers: usize) {
    let num_producers = u64::try_from(num_producers).unwrap();

    group.bench_function("atomic_bag", |b| {
        b.iter_custom(|iters| {
            let (sender, receiver) = mpsc();
            let generate = move || {
                let mut holding_cell = None;
                let mut i = 0;
                loop {
                    holding_cell = if let Some(v) = holding_cell {
                        sender.try_send(v)
                    } else if i < iters {
                        i += 1;
                        sender.try_send(Invalid)
                    } else {
                        break;
                    };
                }
            };

            let producers = (0..num_producers)
                .map(|_| thread::spawn(generate.clone()))
                .collect::<Vec<_>>();
            let result = thread::spawn(move || {
                let start = Instant::now();

                let mut received = 0;
                while received < iters * num_producers {
                    if let Some(Invalid) = receiver.try_recv() {
                        received += 1;
                    }
                }

                start.elapsed()
            })
            .join()
            .unwrap();
            producers.into_iter().for_each(|t| t.join().unwrap());

            result
        });
    });

    group.bench_function("std", |b| {
        b.iter_custom(|iters| {
            let (sender, receiver) = std::sync::mpsc::sync_channel(1);
            let generate = move || {
                let mut holding_cell = None;
                let mut i = 0;
                loop {
                    holding_cell = if let Some(v) = holding_cell {
                        sender.send(v).err().map(|e| e.0)
                    } else if i < iters {
                        i += 1;
                        sender.send(Invalid).err().map(|e| e.0)
                    } else {
                        break;
                    };
                }
            };

            let producers = (0..num_producers)
                .map(|_| thread::spawn(generate.clone()))
                .collect::<Vec<_>>();
            let result = thread::spawn(move || {
                let start = Instant::now();

                let mut received = 0;
                while received < iters * num_producers {
                    if let Ok(Invalid) = receiver.try_recv() {
                        received += 1;
                    }
                }

                start.elapsed()
            })
            .join()
            .unwrap();
            producers.into_iter().for_each(|t| t.join().unwrap());

            result
        });
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().noise_threshold(0.02).warm_up_time(Duration::from_secs(1));
    targets =
    questions,
    single_threaded,
    multi_threaded,
}
criterion_main!(benches);
