# Lockness

The Lockness project aims to provide the fastest parallel algorithms and data structures for niche applications that are
not yet well served by the Rust community (namely Tokio, Rayon, and Crossbeam).

Project crates:

- Lockness Scheduler TODO

# Lockness Scheduler

The Lockness Scheduler is the fastest task scheduler in the world (this statement has not been evaluated by the FDA). It
features a lockless, wait-free core scheduling algorithm designed around cache line message passing. To help users
maximize efficiency, batch task submission and success result elision APIs are available.

## Getting started

TODO

## Brief performance overview

Tokio, rayon, custom crossbeam shit

## This sounds too good to be true

It is.

The Lockness Scheduler makes a fundamental tradeoff: overall task set completion latency (i.e. throughput) is more
important than individual task latency. In other words, using the Lockness Scheduler to serve web requests is (likely) a
bad idea because an individual request may be left waiting indefinitely under load.

Compared to Rayon, the Lockness Scheduler features low level APIs and therefore requires more effort than a
simple `par_iter`.

Additionally, the Lockness Scheduler does not scale: memory usage degrades quadratically with thread count and certain
atomic operations increase linearly with thread count. The author believes this is an acceptable tradeoff in a world
that is unlikely to see memory coherent CPUs featuring more than a few hundred cores.

Finally, the Lockness Scheduler (currently) does not support futures.

## How does it work?

An in-depth blog post detailing the design tradeoffs is available here TODO.
