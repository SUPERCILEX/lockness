use std::{
    ptr::NonNull,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
        mpsc::TrySendError,
    },
    thread,
    time::{Duration, Instant},
};

use criterion::{
    criterion_group, criterion_main, measurement::WallTime, BenchmarkGroup, Criterion,
};
use lockness_bags::{mpsc_slot, Allocated};

#[derive(Clone, Default)]
struct Invalid;

impl Allocated for Invalid {
    fn into_ptr(self) -> NonNull<()> {
        NonNull::dangling()
    }

    unsafe fn from_ptr(ptr: NonNull<()>) -> Self {
        std::hint::black_box(ptr);
        Self
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

#[allow(clippy::too_many_lines)]
fn single_threaded(c: &mut Criterion) {
    fn bench<A, B>(
        group: &mut BenchmarkGroup<WallTime>,
        name: &str,
        mut create: impl FnMut() -> (A, B),
        mut send: impl FnMut(&mut A, Invalid) -> Option<Invalid>,
        mut recv: impl FnMut(&mut B) -> Option<Invalid>,
    ) {
        group.bench_function(format!("{name}/send"), |b| {
            let (mut sender, _) = create();
            b.iter(|| send(&mut sender, Invalid));
        });

        group.bench_function(format!("{name}/receive"), |b| {
            let (_, mut receiver) = create();
            b.iter(|| recv(&mut receiver));
        });

        group.bench_function(format!("{name}/send_receive"), |b| {
            let (mut sender, mut receiver) = create();
            b.iter(|| {
                assert!(send(&mut sender, Invalid).is_none());
                recv(&mut receiver)
            });
        });
    }

    let mut group = c.benchmark_group("single_threaded");

    bench(
        &mut group,
        "atomic_bag",
        mpsc_slot,
        |sender, v| sender.try_send(v),
        |receiver| receiver.try_recv(),
    );

    bench(
        &mut group,
        "std",
        || std::sync::mpsc::sync_channel(1),
        |sender, v| {
            sender.try_send(v).err().map(|e| match e {
                TrySendError::Full(v) | TrySendError::Disconnected(v) => v,
            })
        },
        |receiver| receiver.try_recv().ok(),
    );

    group.bench_function("crossbeam_queue/send", |b| {
        let q = crossbeam_queue::ArrayQueue::new(1);
        b.iter(|| q.push(Invalid));
    });

    group.bench_function("crossbeam_queue/receive", |b| {
        let q = crossbeam_queue::ArrayQueue::<Invalid>::new(1);
        b.iter(|| q.pop());
    });

    group.bench_function("crossbeam_queue/send_receive", |b| {
        let q = crossbeam_queue::ArrayQueue::new(1);
        b.iter(|| {
            q.push(Invalid).ok().unwrap();
            q.pop()
        });
    });

    bench(
        &mut group,
        "crossbeam_channel",
        || crossbeam_channel::bounded(1),
        |sender, v| {
            sender
                .try_send(v)
                .err()
                .map(crossbeam_channel::TrySendError::into_inner)
        },
        |receiver| receiver.try_recv().ok(),
    );

    bench(
        &mut group,
        "flume",
        || flume::bounded(1),
        |sender, v| {
            sender
                .try_send(v)
                .err()
                .map(flume::TrySendError::into_inner)
        },
        |receiver| receiver.try_recv().ok(),
    );

    bench(
        &mut group,
        "kanal",
        || kanal::bounded(1),
        |sender, v| sender.try_send(v).err().map(|_| Invalid),
        |receiver| receiver.try_recv().ok().flatten(),
    );

    bench(
        &mut group,
        "thingbuf",
        || thingbuf::mpsc::channel(1),
        |sender, v| {
            sender
                .try_send(v)
                .err()
                .map(thingbuf::mpsc::errors::TrySendError::into_inner)
        },
        |receiver| receiver.try_recv().ok(),
    );

    bench(
        &mut group,
        "async_channel",
        || async_channel::bounded(1),
        |sender, v| {
            sender
                .try_send(v)
                .err()
                .map(async_channel::TrySendError::into_inner)
        },
        |receiver| receiver.try_recv().ok(),
    );

    group.bench_function("concurrent_queue/send", |b| {
        let q = concurrent_queue::ConcurrentQueue::bounded(1);
        b.iter(|| q.push(Invalid));
    });

    group.bench_function("concurrent_queue/receive", |b| {
        let q = concurrent_queue::ConcurrentQueue::<Invalid>::bounded(1);
        b.iter(|| q.pop());
    });

    group.bench_function("concurrent_queue/send_receive", |b| {
        let q = concurrent_queue::ConcurrentQueue::bounded(1);
        b.iter(|| {
            q.push(Invalid).ok().unwrap();
            q.pop()
        });
    });

    bench(
        &mut group,
        "ringbuf",
        || ringbuf::StaticRb::<_, 1>::default().split(),
        |sender, v| sender.push(v).err(),
        ringbuf::Consumer::pop,
    );
}

fn multi_threaded(c: &mut Criterion) {
    let mut threads = 2;
    let available = thread::available_parallelism().unwrap().get();

    while threads <= available {
        if threads == 4 {
            let mut group = c.benchmark_group("3_threads");
            mpsc_(&mut group, 2);
        }

        let mut group = c.benchmark_group(format!("{threads}_threads"));

        if threads == 2 {
            spsc_(&mut group);
        }
        mpsc_(&mut group, threads - 1);
        threads *= 2;
    }
}

fn spsc_(group: &mut BenchmarkGroup<WallTime>) {
    fn bench(
        iters: u64,
        mut send: impl FnMut(Invalid) -> Option<Invalid> + Send,
        mut recv: impl FnMut() -> Option<Invalid> + Send,
    ) -> Duration {
        thread::scope(|scope| {
            scope.spawn(|| {
                let mut holding_cell = None;
                let mut i = 0;
                loop {
                    holding_cell = if let Some(v) = holding_cell {
                        send(v)
                    } else if i < iters {
                        i += 1;
                        send(Invalid)
                    } else {
                        break;
                    };
                }
            });
            let result = scope
                .spawn(move || {
                    let start = Instant::now();

                    let mut received = 0;
                    while received < iters {
                        if matches!(recv(), Some(Invalid)) {
                            received += 1;
                        }
                    }

                    start.elapsed()
                })
                .join()
                .unwrap();

            result
        })
    }

    group.bench_function("ringbuf", |b| {
        b.iter_custom(|iters| {
            let mut q = ringbuf::StaticRb::<_, 1>::default();
            let (mut sender, mut receiver) = q.split_ref();
            bench(iters, |v| sender.push(v).err(), || receiver.pop())
        });
    });
}

#[allow(clippy::too_many_lines)]
fn mpsc_(group: &mut BenchmarkGroup<WallTime>, num_producers: usize) {
    fn bench(
        num_producers: u64,
        iters: u64,
        mut send: impl FnMut(Invalid) -> Option<Invalid> + Clone + Send,
        mut recv: impl FnMut() -> Option<Invalid> + Send,
    ) -> Duration {
        let generate = move || {
            let mut holding_cell = None;
            let mut i = 0;
            loop {
                holding_cell = if let Some(v) = holding_cell {
                    send(v)
                } else if i < iters {
                    i += 1;
                    send(Invalid)
                } else {
                    break;
                };
            }
        };

        thread::scope(|scope| {
            let producers = (0..num_producers)
                .map(|_| scope.spawn(generate.clone()))
                .collect::<Vec<_>>();
            let result = scope
                .spawn(move || {
                    let start = Instant::now();

                    let mut received = 0;
                    while received < iters * num_producers {
                        if matches!(recv(), Some(Invalid)) {
                            received += 1;
                        }
                    }

                    start.elapsed()
                })
                .join()
                .unwrap();
            producers.into_iter().for_each(|t| t.join().unwrap());

            result
        })
    }

    let num_producers = u64::try_from(num_producers).unwrap();

    group.bench_function("atomic_bag", |b| {
        b.iter_custom(|iters| {
            let (sender, receiver) = mpsc_slot();
            bench(
                num_producers,
                iters,
                move |v| sender.try_send(v),
                move || receiver.try_recv(),
            )
        });
    });

    group.bench_function("std", |b| {
        b.iter_custom(|iters| {
            let (sender, receiver) = std::sync::mpsc::sync_channel(1);
            bench(
                num_producers,
                iters,
                move |v| {
                    sender.try_send(v).err().map(|e| match e {
                        TrySendError::Full(v) | TrySendError::Disconnected(v) => v,
                    })
                },
                move || receiver.try_recv().ok(),
            )
        });
    });

    group.bench_function("crossbeam_queue", |b| {
        b.iter_custom(|iters| {
            let q = crossbeam_queue::ArrayQueue::new(1);
            bench(num_producers, iters, |v| q.push(v).err(), || q.pop())
        });
    });

    group.bench_function("crossbeam_channel", |b| {
        b.iter_custom(|iters| {
            let (sender, receiver) = crossbeam_channel::bounded(1);
            bench(
                num_producers,
                iters,
                move |v| {
                    sender
                        .try_send(v)
                        .err()
                        .map(crossbeam_channel::TrySendError::into_inner)
                },
                move || receiver.try_recv().ok(),
            )
        });
    });

    group.bench_function("flume", |b| {
        b.iter_custom(|iters| {
            let (sender, receiver) = flume::bounded(1);
            bench(
                num_producers,
                iters,
                move |v| {
                    sender
                        .try_send(v)
                        .err()
                        .map(flume::TrySendError::into_inner)
                },
                move || receiver.try_recv().ok(),
            )
        });
    });

    group.bench_function("kanal", |b| {
        b.iter_custom(|iters| {
            let (sender, receiver) = kanal::bounded(1);
            bench(
                num_producers,
                iters,
                move |v| sender.send(v).err().map(|_| Invalid),
                move || receiver.try_recv().ok().flatten(),
            )
        });
    });

    group.bench_function("thingbuf", |b| {
        b.iter_custom(|iters| {
            let (sender, receiver) = thingbuf::mpsc::channel(1);
            bench(
                num_producers,
                iters,
                move |v| {
                    sender
                        .try_send(v)
                        .err()
                        .map(thingbuf::mpsc::errors::TrySendError::into_inner)
                },
                move || receiver.try_recv().ok(),
            )
        });
    });

    group.bench_function("async_channel", |b| {
        b.iter_custom(|iters| {
            let (sender, receiver) = async_channel::bounded(1);
            bench(
                num_producers,
                iters,
                move |v| {
                    sender
                        .try_send(v)
                        .err()
                        .map(async_channel::TrySendError::into_inner)
                },
                move || receiver.try_recv().ok(),
            )
        });
    });

    group.bench_function("concurrent_queue", |b| {
        b.iter_custom(|iters| {
            let q = concurrent_queue::ConcurrentQueue::bounded(1);
            bench(
                num_producers,
                iters,
                |v| q.push(v).err().map(concurrent_queue::PushError::into_inner),
                || q.pop().ok(),
            )
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
