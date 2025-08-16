# Lockness

The Lockness project aims provide high-performance blocking concurrency APIs.

TODO

Project crates:

- [`executor/`](executor): The Lockness task scheduler (think tokio without async).
- [`bags/`](bags): Blocking message passing primitives (i.e. channels).
- [`vecs/`](vecs): Type erased vectors (used to pass around task closures).
