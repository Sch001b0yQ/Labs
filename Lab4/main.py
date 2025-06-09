import argparse, random, time, os
from concurrent.futures import ProcessPoolExecutor

def partial_sum(data_slice):
    """data_slice – зріз списку (list[float]); повертає суму"""
    return sum(data_slice)

def fork_join_sum(arr, threshold, max_workers=None):

    tasks = [(i, min(i + threshold, len(arr)))
             for i in range(0, len(arr), threshold)]

    t0 = time.perf_counter()
    with ProcessPoolExecutor(max_workers=max_workers) as pool:
        futures = [pool.submit(partial_sum, arr[lo:hi]) for lo, hi in tasks]
        total = sum(f.result() for f in futures)
    return total, time.perf_counter() - t0

def seq_sum(arr):
    t0 = time.perf_counter()
    return sum(arr), time.perf_counter() - t0

def main():
    ap = argparse.ArgumentParser(description="Fork/Join-style sum vs sequential")
    ap.add_argument("--mode", choices=["seq", "fj"], default="fj")
    ap.add_argument("--n", type=int, default=10_000_000, help="розмір масиву")
    ap.add_argument("--threshold", type=int, default=250_000,
                    help="скільки елементів рахує один процес")
    ap.add_argument("--procs", type=int, default=os.cpu_count(),
                    help="кількість процесів (default = CPU cores)")
    args = ap.parse_args()

    random.seed(42)
    data = [random.random() for _ in range(args.n)]

    if args.mode == "seq":
        s, t = seq_sum(data)
        print(f"[SEQ]  сума={s:.4f}  |  {t:.3f} c")
    else:
        s, t = fork_join_sum(data, args.threshold, args.procs)
        print(f"[FJ]   сума={s:.4f}  |  {t:.3f} c  "
              f"(threshold={args.threshold}, procs={args.procs})")

if __name__ == "__main__":
    main()
