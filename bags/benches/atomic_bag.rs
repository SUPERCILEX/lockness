#![feature(mpmc_channel)]
#![allow(clippy::needless_pass_by_value)]

use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
        mpsc::{SendError, TrySendError},
    },
    thread,
    time::{Duration, Instant},
};

use arrayvec::ArrayVec;
use criterion::{
    BenchmarkGroup, Criterion, criterion_group, criterion_main, measurement::WallTime,
};
use lockness_bags::{MpmcSender, mpmc as lockness_mpmc, mpsc_slot};
use ringbuf::{
    consumer::Consumer,
    producer::Producer,
    traits::{Split, SplitRef},
};

fn questions(c: &mut Criterion) {
    let mut group = c.benchmark_group("questions");

    group.bench_function("swap", |b| {
        b.iter_custom(|iters| {
            let slot = AtomicU64::new(0);
            thread::scope(|scope| {
                let producer = scope.spawn(|| {
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

                producer.join().unwrap();
                consumer.join().unwrap()
            })
        });
    });

    group.bench_function("load_store", |b| {
        b.iter_custom(|iters| {
            let flag = AtomicBool::new(false);
            let slot = AtomicU64::new(0);
            thread::scope(|scope| {
                let producer = scope.spawn(|| {
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

                producer.join().unwrap();
                consumer.join().unwrap()
            })
        });
    });
}

trait SpscBencher {
    fn bench<
        T: Send,
        Sender,
        Receiver,
        Name: AsRef<str>,
        Create: FnMut() -> (Sender, Receiver),
        Make: FnMut() -> T,
        Send_: FnMut(&mut Sender, T) -> Option<T>,
        Receive: FnMut(&mut Receiver) -> Option<T>,
    >(
        &self,
        group: &mut BenchmarkGroup<WallTime>,
        name: Name,
        create: Create,
        make: Make,
        send: Send_,
        recv: Receive,
    );
}

trait MpscBencher {
    fn bench<
        T: Send,
        Sender: Send + Clone,
        Receiver: Send,
        Name: AsRef<str>,
        Create: FnMut() -> (Sender, Receiver),
        Make: FnMut() -> T + Clone + Send,
        Send_: FnMut(&mut Sender, T) -> Option<T> + Clone + Send,
        Receive: FnMut(&mut Receiver) -> Option<T> + Clone + Send,
    >(
        &self,
        group: &mut BenchmarkGroup<WallTime>,
        name: Name,
        create: Create,
        make: Make,
        send: Send_,
        recv: Receive,
    );
}

trait MpmcBencher {
    fn bench<
        T: Send,
        Sender: Send + Clone,
        Receiver: Send + Clone,
        Name: AsRef<str>,
        Create: FnMut() -> (Sender, Receiver),
        Make: FnMut() -> T + Clone + Send,
        Send_: FnMut(&mut Sender, T) -> Option<T> + Clone + Send,
        Receive: FnMut(&mut Receiver) -> Option<T> + Clone + Send,
    >(
        &self,
        group: &mut BenchmarkGroup<WallTime>,
        name: Name,
        create: Create,
        make: Make,
        send: Send_,
        recv: Receive,
    );
}

#[allow(clippy::too_many_lines)]
fn spsc_impls(group: &mut BenchmarkGroup<WallTime>, bencher: impl SpscBencher) {
    bencher.bench(
        group,
        "ringbuf(1)",
        || ringbuf::StaticRb::<_, 1>::default().split(),
        || std::hint::black_box(42),
        |sender, v| sender.try_push(v).err(),
        |receiver| receiver.try_pop(),
    );

    bencher.bench(
        group,
        "ringbuf(32)",
        || ringbuf::StaticRb::<_, 32>::default().split(),
        || std::hint::black_box(42),
        |sender, v| sender.try_push(v).err(),
        |receiver| receiver.try_pop(),
    );
}

#[allow(clippy::too_many_lines)]
fn mpsc_impls(group: &mut BenchmarkGroup<WallTime>, bencher: impl MpscBencher) {
    bencher.bench(
        group,
        "lockness_mpsc",
        mpsc_slot,
        || std::hint::black_box(42),
        |sender, v| match sender.try_send(v) {
            Ok(()) => None,
            Err(TrySendError::Full(v) | TrySendError::Disconnected(v)) => Some(v),
        },
        |receiver| receiver.try_recv().ok(),
    );

    bencher.bench(
        group,
        "std_mpsc",
        std::sync::mpsc::channel,
        || std::hint::black_box(42),
        |sender, v| sender.send(v).err().map(|SendError(e)| e),
        |receiver| receiver.try_recv().ok(),
    );

    let mut parameterized = |capacity| {
        bencher.bench(
            group,
            format!("std_mpsc({capacity})"),
            || std::sync::mpsc::sync_channel(capacity),
            || std::hint::black_box(42),
            |sender, v| {
                sender.try_send(v).err().map(|e| match e {
                    TrySendError::Full(v) | TrySendError::Disconnected(v) => v,
                })
            },
            |receiver| receiver.try_recv().ok(),
        );

        bencher.bench(
            group,
            format!("crossbeam_channel({capacity})"),
            || crossbeam_channel::bounded(capacity),
            || std::hint::black_box(42),
            |sender, v| {
                sender
                    .try_send(v)
                    .err()
                    .map(crossbeam_channel::TrySendError::into_inner)
            },
            |receiver| receiver.try_recv().ok(),
        );

        bencher.bench(
            group,
            format!("flume({capacity})"),
            || flume::bounded(capacity),
            || std::hint::black_box(42),
            |sender, v| {
                sender
                    .try_send(v)
                    .err()
                    .map(flume::TrySendError::into_inner)
            },
            |receiver| receiver.try_recv().ok(),
        );

        bencher.bench(
            group,
            format!("thingbuf({capacity})"),
            || thingbuf::mpsc::channel(capacity),
            || std::hint::black_box(42),
            |sender, v| {
                sender
                    .try_send(v)
                    .err()
                    .map(thingbuf::mpsc::errors::TrySendError::into_inner)
            },
            |receiver| receiver.try_recv().ok(),
        );
    };

    parameterized(1);
    parameterized(32);
}

#[allow(clippy::too_many_lines)]
fn mpmc_impls(group: &mut BenchmarkGroup<WallTime>, bencher: impl MpmcBencher) {
    bencher.bench(
        group,
        "lockness_mpmc_single_send",
        || {
            let (sender, receiver) = lockness_mpmc();
            (sender, (receiver, ArrayVec::<i32, 30>::new_const()))
        },
        || std::hint::black_box(42),
        |sender, v| match sender.try_send(v) {
            Ok(()) => None,
            Err(TrySendError::Full(v) | TrySendError::Disconnected(v)) => Some(v),
        },
        |(receiver, buf)| {
            buf.pop().or_else(|| {
                receiver.try_recv().ok().and_then(|nums| {
                    *buf = nums;
                    buf.pop()
                })
            })
        },
    );

    {
        #[derive(Clone)]
        struct SendBuf<const N: usize, T>(MpmcSender<T>, ArrayVec<T, N>);

        impl<const N: usize, T> Drop for SendBuf<N, T> {
            fn drop(&mut self) {
                let Self(sender, buf) = self;
                while !buf.is_empty() {
                    let (Ok(_) | Err(_)) = sender.try_send(&mut *buf);
                }
            }
        }

        bencher.bench(
            group,
            "lockness_mpmc_full_send",
            || {
                let (sender, receiver) = lockness_mpmc();
                (
                    SendBuf(sender, ArrayVec::<i32, 30>::new_const()),
                    (receiver, ArrayVec::<i32, 30>::new_const()),
                )
            },
            || std::hint::black_box(42),
            |SendBuf(sender, buf), v| {
                if let Err(e) = buf.try_push(v) {
                    let (Ok(_) | Err(_)) = sender.try_send(&mut *buf);
                    Some(e.element())
                } else {
                    None
                }
            },
            |(receiver, buf)| {
                buf.pop().or_else(|| {
                    receiver.try_recv().ok().and_then(|nums| {
                        *buf = nums;
                        buf.pop()
                    })
                })
            },
        );
    }

    bencher.bench(
        group,
        "std_mpmc",
        std::sync::mpmc::channel,
        || std::hint::black_box(42),
        |sender, v| sender.send(v).err().map(|SendError(e)| e),
        |receiver| receiver.try_recv().ok(),
    );

    let mut parameterized = |capacity| {
        bencher.bench(
            group,
            format!("std_mpmc({capacity})"),
            || std::sync::mpmc::sync_channel(capacity),
            || std::hint::black_box(42),
            |sender, v| {
                sender.try_send(v).err().map(|e| match e {
                    TrySendError::Full(v) | TrySendError::Disconnected(v) => v,
                })
            },
            |receiver| receiver.try_recv().ok(),
        );

        {
            let q = crossbeam_queue::ArrayQueue::new(capacity);
            bencher.bench(
                group,
                format!("crossbeam_queue({capacity})"),
                || (&q, &q),
                || std::hint::black_box(42),
                |sender, v| sender.push(v).err(),
                |receiver| receiver.pop(),
            );
        }

        bencher.bench(
            group,
            format!("async_channel({capacity})"),
            || async_channel::bounded(capacity),
            || std::hint::black_box(42),
            |sender, v| {
                sender
                    .try_send(v)
                    .err()
                    .map(async_channel::TrySendError::into_inner)
            },
            |receiver| receiver.try_recv().ok(),
        );

        {
            let q = concurrent_queue::ConcurrentQueue::bounded(capacity);
            bencher.bench(
                group,
                format!("concurrent_queue({capacity})"),
                || (&q, &q),
                || std::hint::black_box(42),
                |sender, v| {
                    sender
                        .push(v)
                        .err()
                        .map(concurrent_queue::PushError::into_inner)
                },
                |receiver| receiver.pop().ok(),
            );
        }
    };

    parameterized(1);
    parameterized(32);
}

fn single_threaded(c: &mut Criterion) {
    struct Spsc;

    impl SpscBencher for Spsc {
        fn bench<
            T: Send,
            Sender,
            Receiver,
            Name: AsRef<str>,
            Create: FnMut() -> (Sender, Receiver),
            Make: FnMut() -> T,
            Send_: FnMut(&mut Sender, T) -> Option<T>,
            Receive: FnMut(&mut Receiver) -> Option<T>,
        >(
            &self,
            group: &mut BenchmarkGroup<WallTime>,
            name: Name,
            mut create: Create,
            mut make: Make,
            mut send: Send_,
            mut recv: Receive,
        ) {
            let name = name.as_ref();

            group.bench_function(format!("{name}/send"), |b| {
                let (mut sender, _receiver) = create();
                b.iter(|| send(&mut sender, make()));
            });

            group.bench_function(format!("{name}/receive"), |b| {
                let (_sender, mut receiver) = create();
                b.iter(|| recv(&mut receiver));
            });

            group.bench_function(format!("{name}/send_receive"), |b| {
                let (mut sender, mut receiver) = create();
                b.iter(|| {
                    assert!(send(&mut sender, make()).is_none());
                    recv(&mut receiver)
                });
            });
        }
    }

    impl MpscBencher for Spsc {
        fn bench<
            T: Send,
            Sender,
            Receiver,
            Name: AsRef<str>,
            Create: FnMut() -> (Sender, Receiver),
            Make: FnMut() -> T,
            Send_: FnMut(&mut Sender, T) -> Option<T>,
            Receive: FnMut(&mut Receiver) -> Option<T>,
        >(
            &self,
            group: &mut BenchmarkGroup<WallTime>,
            name: Name,
            create: Create,
            make: Make,
            send: Send_,
            recv: Receive,
        ) {
            SpscBencher::bench(self, group, name, create, make, send, recv);
        }
    }

    impl MpmcBencher for Spsc {
        fn bench<
            T: Send,
            Sender,
            Receiver,
            Name: AsRef<str>,
            Create: FnMut() -> (Sender, Receiver),
            Make: FnMut() -> T,
            Send_: FnMut(&mut Sender, T) -> Option<T>,
            Receive: FnMut(&mut Receiver) -> Option<T>,
        >(
            &self,
            group: &mut BenchmarkGroup<WallTime>,
            name: Name,
            create: Create,
            make: Make,
            send: Send_,
            recv: Receive,
        ) {
            SpscBencher::bench(self, group, name, create, make, send, recv);
        }
    }

    let mut group = c.benchmark_group("single_threaded");

    spsc_impls(&mut group, Spsc);
    mpsc_impls(&mut group, Spsc);
    mpmc_impls(&mut group, Spsc);
}

fn multi_threaded(c: &mut Criterion) {
    let mut threads = 2;
    let available = thread::available_parallelism().unwrap().get() * 2;

    while threads <= available {
        if threads == 2 {
            let mut group = c.benchmark_group("1_producers");
            spsc_(&mut group);
        }
        if threads == 4 {
            let mut group = c.benchmark_group("2_producers");
            mpsc_(&mut group, 2);
        }

        {
            let num_producers = threads - 1;
            let mut group = c.benchmark_group(format!("{num_producers}_producers"));
            mpsc_(&mut group, num_producers);
        }
        {
            let mut group = c.benchmark_group(format!("{threads}_threads"));
            mpmc_(&mut group, threads);
        }
        threads *= 2;
    }
}

fn spsc_(group: &mut BenchmarkGroup<WallTime>) {
    fn bench(
        iters: u64,
        mut send: impl FnMut(usize) -> Option<usize> + Send,
        mut recv: impl FnMut() -> Option<usize> + Send,
    ) -> Duration {
        thread::scope(|scope| {
            let producer = scope.spawn(|| {
                let mut holding_cell = None;
                let mut i = 0;
                loop {
                    holding_cell = if let Some(v) = holding_cell {
                        send(v)
                    } else if i < iters {
                        i += 1;
                        send(std::hint::black_box(42))
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
                        if recv().is_some() {
                            received += 1;
                        }
                    }

                    start.elapsed()
                })
                .join()
                .unwrap();

            producer.join().unwrap();

            result
        })
    }

    group.bench_function("ringbuf(1)", |b| {
        b.iter_custom(|iters| {
            let mut q = ringbuf::StaticRb::<_, 1>::default();
            let (mut sender, mut receiver) = q.split_ref();
            bench(iters, |v| sender.try_push(v).err(), || receiver.try_pop())
        });
    });

    group.bench_function("ringbuf(32)", |b| {
        b.iter_custom(|iters| {
            let mut q = ringbuf::StaticRb::<_, 32>::default();
            let (mut sender, mut receiver) = q.split_ref();
            bench(iters, |v| sender.try_push(v).err(), || receiver.try_pop())
        });
    });
}

#[allow(clippy::too_many_lines)]
fn mpsc_(group: &mut BenchmarkGroup<WallTime>, num_producers: usize) {
    fn bench<T: Send>(
        num_producers: u64,
        iters: u64,
        make_tea: impl FnMut() -> T + Clone + Send,
        send: impl FnMut(T) -> Option<T> + Clone + Send,
        mut recv: impl FnMut() -> Option<T> + Send,
    ) -> Duration {
        thread::scope(|scope| {
            let producers = (0..num_producers)
                .map(|_| {
                    let mut make_tea = make_tea.clone();
                    let mut send = send.clone();
                    scope.spawn(move || {
                        let mut holding_cell = None;
                        let mut i = 0;
                        loop {
                            holding_cell = if let Some(v) = holding_cell {
                                send(v)
                            } else if i < iters {
                                i += 1;
                                send(make_tea())
                            } else {
                                break;
                            };
                        }
                    })
                })
                .collect::<Vec<_>>();

            let result = scope.spawn(move || {
                let start = Instant::now();

                let mut received = 0;
                while received < iters * num_producers {
                    if recv().is_some() {
                        received += 1;
                    }
                }

                start.elapsed()
            });

            for producer in producers {
                producer.join().unwrap();
            }

            result.join().unwrap()
        })
    }

    #[derive(Copy, Clone)]
    struct Mpsc {
        num_producers: u64,
    }

    impl MpscBencher for Mpsc {
        fn bench<
            T: Send,
            Sender: Send + Clone,
            Receiver: Send,
            Name: AsRef<str>,
            Create: FnMut() -> (Sender, Receiver),
            Make: FnMut() -> T + Clone + Send,
            Send_: FnMut(&mut Sender, T) -> Option<T> + Clone + Send,
            Receive: FnMut(&mut Receiver) -> Option<T> + Clone + Send,
        >(
            &self,
            group: &mut BenchmarkGroup<WallTime>,
            name: Name,
            mut create: Create,
            make: Make,
            send: Send_,
            recv: Receive,
        ) {
            group.bench_function(name.as_ref(), |b| {
                b.iter_custom(|iters| {
                    let mut send = send.clone();
                    let mut recv = recv.clone();
                    let (mut sender, mut receiver) = create();
                    let make = make.clone();
                    bench(
                        self.num_producers,
                        iters,
                        make,
                        move |v| send(&mut sender, v),
                        move || recv(&mut receiver),
                    )
                });
            });
        }
    }

    impl MpmcBencher for Mpsc {
        fn bench<
            T: Send,
            Sender: Send + Clone,
            Receiver: Send,
            Name: AsRef<str>,
            Create: FnMut() -> (Sender, Receiver),
            Make: FnMut() -> T + Clone + Send,
            Send_: FnMut(&mut Sender, T) -> Option<T> + Clone + Send,
            Receive: FnMut(&mut Receiver) -> Option<T> + Clone + Send,
        >(
            &self,
            group: &mut BenchmarkGroup<WallTime>,
            name: Name,
            create: Create,
            make: Make,
            send: Send_,
            recv: Receive,
        ) {
            MpscBencher::bench(self, group, name, create, make, send, recv)
        }
    }

    let mpsc = Mpsc {
        num_producers: u64::try_from(num_producers).unwrap(),
    };
    mpsc_impls(group, mpsc);
    mpmc_impls(group, mpsc);
}

#[allow(clippy::too_many_lines)]
fn mpmc_(group: &mut BenchmarkGroup<WallTime>, num_threads: usize) {
    fn bench<T: Send>(
        num_threads: u64,
        iters: u64,
        make_tea: impl FnMut() -> T + Clone + Send,
        send: impl FnMut(T) -> Option<T> + Clone + Send,
        recv: impl FnMut() -> Option<T> + Clone + Send,
    ) -> Duration {
        let die = AtomicBool::new(false);
        thread::scope(|scope| {
            let start = Instant::now();
            let producers = (0..num_threads / 2)
                .map(|_| {
                    let mut make_tea = make_tea.clone();
                    let mut send = send.clone();
                    scope.spawn(move || {
                        let mut holding_cell = None;
                        let mut i = 0;
                        loop {
                            holding_cell = if let Some(v) = holding_cell {
                                send(v)
                            } else if i < iters {
                                i += 1;
                                send(make_tea())
                            } else {
                                break;
                            };
                        }
                    })
                })
                .collect::<Vec<_>>();

            let consumers = (0..num_threads / 2)
                .map(|_| {
                    let mut recv = recv.clone();
                    let die = &die;
                    scope.spawn(move || {
                        while !die.load(Relaxed) {
                            std::hint::black_box(recv());
                        }
                    })
                })
                .collect::<Vec<_>>();

            for producer in producers {
                producer.join().unwrap();
            }
            die.store(true, Relaxed);
            for consumer in consumers {
                consumer.join().unwrap();
            }

            start.elapsed()
        })
    }

    struct Mpmc {
        num_threads: u64,
    }

    impl MpmcBencher for Mpmc {
        fn bench<
            T: Send,
            Sender: Send + Clone,
            Receiver: Send + Clone,
            Name: AsRef<str>,
            Create: FnMut() -> (Sender, Receiver),
            Make: FnMut() -> T + Clone + Send,
            Send_: FnMut(&mut Sender, T) -> Option<T> + Clone + Send,
            Receive: FnMut(&mut Receiver) -> Option<T> + Clone + Send,
        >(
            &self,
            group: &mut BenchmarkGroup<WallTime>,
            name: Name,
            mut create: Create,
            make: Make,
            send: Send_,
            recv: Receive,
        ) {
            group.bench_function(name.as_ref(), |b| {
                b.iter_custom(|iters| {
                    let mut send = send.clone();
                    let mut recv = recv.clone();
                    let (mut sender, mut receiver) = create();
                    let make = make.clone();
                    bench(
                        self.num_threads,
                        iters,
                        make,
                        move |v| send(&mut sender, v),
                        move || recv(&mut receiver),
                    )
                });
            });
        }
    }

    mpmc_impls(group, Mpmc {
        num_threads: u64::try_from(num_threads).unwrap(),
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
