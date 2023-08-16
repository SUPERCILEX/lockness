use std::{mem, time::Duration};

use criterion::{
    criterion_group, criterion_main, measurement::Measurement, AxisScale, BatchSize,
    BenchmarkGroup, BenchmarkId, Criterion, PlotConfiguration, Throughput,
};
use lockness_core::closure_vec::ClosureVec;

fn bench_all(group: &mut BenchmarkGroup<impl Measurement>, f: impl FnOnce() + Clone + 'static) {
    bench_create(group, f.clone());
    bench_push(group, f.clone());
    bench_pop(group, f.clone());
    bench_drop(group, f);
}

fn bench_create(group: &mut BenchmarkGroup<impl Measurement>, f: impl FnOnce() + Clone + 'static) {
    group.bench_function(BenchmarkId::new("create", 1), |b| {
        let f = f.clone();
        b.iter(|| {
            let mut tasks = ClosureVec::new();
            tasks.push(f.clone());
            tasks
        });
    });
}

fn bench_push(group: &mut BenchmarkGroup<impl Measurement>, f: impl FnOnce() + Clone + 'static) {
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));

    for num_elems in [1 << 10, 1 << 15, 1 << 20] {
        group.throughput(Throughput::Elements(num_elems));
        group.bench_with_input(
            BenchmarkId::new("push", num_elems),
            &num_elems,
            |b, &num_elems| {
                let f = f.clone();
                b.iter_with_large_drop(|| {
                    let mut tasks = ClosureVec::new();
                    for _ in 0..num_elems {
                        tasks.push(f.clone());
                    }
                    tasks
                });
            },
        );
    }
}

fn bench_pop(group: &mut BenchmarkGroup<impl Measurement>, f: impl FnOnce() + Clone + 'static) {
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));

    for num_elems in [1 << 10, 1 << 15, 1 << 20] {
        group.throughput(Throughput::Elements(num_elems));
        group.bench_with_input(
            BenchmarkId::new("pop", num_elems),
            &num_elems,
            |b, &num_elems| {
                let f = f.clone();
                b.iter_batched_ref(
                    || {
                        let mut tasks = ClosureVec::new();
                        for _ in 0..num_elems {
                            tasks.push(f.clone());
                        }
                        tasks
                    },
                    |tasks| {
                        while !tasks.pop_and_run() {}
                    },
                    BatchSize::LargeInput,
                );
            },
        );
    }
}

fn bench_drop(group: &mut BenchmarkGroup<impl Measurement>, f: impl FnOnce() + Clone + 'static) {
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));

    for num_elems in [1 << 10, 1 << 15, 1 << 20] {
        group.throughput(Throughput::Elements(num_elems));
        group.bench_with_input(
            BenchmarkId::new("drop", num_elems),
            &num_elems,
            |b, &num_elems| {
                let f = f.clone();
                b.iter_batched(
                    || {
                        let mut tasks = ClosureVec::new();
                        for _ in 0..num_elems {
                            tasks.push(f.clone());
                        }
                        tasks
                    },
                    drop,
                    BatchSize::LargeInput,
                );
            },
        );
    }
}

fn zst(c: &mut Criterion) {
    let mut group = c.benchmark_group("zst");

    let f = || std::hint::black_box(());
    assert_eq!(0, mem::size_of_val(&f));

    bench_all(&mut group, f);
}

fn normal(c: &mut Criterion) {
    let mut group = c.benchmark_group("normal");

    let arr = [1u64, 2, 3, 4];
    let f = move || {
        std::hint::black_box(arr);
    };
    assert_eq!(32, mem::size_of_val(&f));

    bench_all(&mut group, f);
}

fn big(c: &mut Criterion) {
    let mut group = c.benchmark_group("big");

    let arr = [0xDEAD_BEEF_u64; 512];
    let f = move || {
        std::hint::black_box(arr);
    };
    assert_eq!(4096, mem::size_of_val(&f));

    bench_all(&mut group, f);
}

criterion_group! {
    name = benches;
    config = Criterion::default().noise_threshold(0.02).warm_up_time(Duration::from_secs(1));
    targets =
    zst,
    normal,
    big,
}
criterion_main!(benches);
