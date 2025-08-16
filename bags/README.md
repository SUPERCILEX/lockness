# Lockness Bags

Lockness Bags are a novel message passing architecture based on unordered, fixed-size bitvectors.
Full details are available
[on my blog](https://alexsaveau.dev/blog/opinions/performance/lockness/lockless-queues-are-not-queues).

They are not recommended for production use as they do not consistently outperform crossbeam
channels. However, I believe bags could be *the* fastest channel implementation given
[hardware acceleration](https://alexsaveau.dev/blog/opinions/performance/lockness/atomic-bit-fill).
