"""Quick performance benchmark for fake_firestore operations."""

import time

from fake_firestore import FakeFirestoreClient


def bench_set_batch(sizes: list[int]) -> None:
    """Benchmark set() for different batch sizes."""
    print("=== set() batch ===")
    for n in sizes:
        fs = FakeFirestoreClient()
        start = time.perf_counter()
        for i in range(n):
            fs.collection("bench").document(f"doc_{i}").set(
                {"name": f"user_{i}", "score": i, "nested": {"a": 1, "b": 2}}
            )
        elapsed = time.perf_counter() - start
        print(f"  x{n:>5}: {elapsed:.4f}s  ({elapsed / n * 1000:.3f} ms/op)")


def bench_set_degradation(n: int) -> None:
    """Check if set() slows down as the collection grows."""
    print(f"\n=== set() degradation (up to {n} docs) ===")
    fs = FakeFirestoreClient()
    timings: list[float] = []
    for i in range(n):
        start = time.perf_counter()
        fs.collection("bench").document(f"doc_{i}").set({"name": f"user_{i}", "score": i})
        timings.append(time.perf_counter() - start)

    for checkpoint in [1, 100, 500, 1000, 2000]:
        if checkpoint > n:
            break
        window = timings[max(0, checkpoint - 10) : checkpoint]
        avg = sum(window) / len(window)
        print(f"  at size {checkpoint:>5}: avg {avg * 1000:.3f} ms/op (last 10)")


def bench_get(collection_size: int, reads: int) -> None:
    """Benchmark get() on an existing collection."""
    print(f"\n=== get() x{reads} (collection size {collection_size}) ===")
    fs = FakeFirestoreClient()
    for i in range(collection_size):
        fs.collection("bench").document(f"doc_{i}").set({"name": f"user_{i}", "score": i})

    start = time.perf_counter()
    for i in range(reads):
        fs.collection("bench").document(f"doc_{i % collection_size}").get()
    elapsed = time.perf_counter() - start
    print(f"  {elapsed:.4f}s  ({elapsed / reads * 1000:.3f} ms/op)")


def bench_stream(collection_size: int) -> None:
    """Benchmark stream() on a collection."""
    print(f"\n=== stream() (collection size {collection_size}) ===")
    fs = FakeFirestoreClient()
    for i in range(collection_size):
        fs.collection("bench").document(f"doc_{i}").set({"name": f"user_{i}", "score": i})

    start = time.perf_counter()
    docs = list(fs.collection("bench").stream())
    elapsed = time.perf_counter() - start
    print(f"  {len(docs)} docs in {elapsed:.4f}s")


def bench_where(collection_size: int) -> None:
    """Benchmark where() query on a collection."""
    print(f"\n=== where() (collection size {collection_size}) ===")
    fs = FakeFirestoreClient()
    for i in range(collection_size):
        fs.collection("bench").document(f"doc_{i}").set({"name": f"user_{i}", "score": i})

    start = time.perf_counter()
    docs = list(fs.collection("bench").where("score", ">=", collection_size // 2).stream())
    elapsed = time.perf_counter() - start
    print(f"  {len(docs)} matched in {elapsed:.4f}s")


if __name__ == "__main__":
    bench_set_batch([100, 500, 1000, 2000])
    bench_set_degradation(2000)
    bench_get(1000, 1000)
    bench_stream(1000)
    bench_where(1000)
