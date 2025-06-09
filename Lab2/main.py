import argparse, queue, threading, random, time
from pathlib import Path

def run_pc(n_prod, n_cons, q_size, items_per_prod):
    q = queue.Queue(q_size)
    summary = {"sum": 0.0, "cnt": 0, "lock": threading.Lock()}

    def producer():
        rnd = random.Random()
        for _ in range(items_per_prod):
            q.put(rnd.random())
        q.put(None)

    def consumer():
        while True:
            item = q.get()
            if item is None:
                q.put(None); q.task_done(); break
            with summary["lock"]:
                summary["sum"] += item; summary["cnt"] += 1
            q.task_done()

    prods = [threading.Thread(target=producer, daemon=True) for _ in range(n_prod)]
    cons  = [threading.Thread(target=consumer, daemon=True) for _ in range(n_cons)]
    t0 = time.perf_counter()
    [t.start() for t in prods + cons]
    q.join()
    elapsed = time.perf_counter() - t0
    return elapsed, summary["cnt"], summary["sum"] / summary["cnt"]


def run_pipeline(n_prod, n_proc, n_agg, q_size, items_per_prod):
    q1, q2 = queue.Queue(q_size), queue.Queue(q_size)
    result = {"sum": 0.0, "cnt": 0, "lock": threading.Lock()}

    def producer():
        rnd = random.Random()
        for _ in range(items_per_prod):
            q1.put(rnd.random())
        q1.put(None)

    def processor():
        while True:
            x = q1.get()
            if x is None:
                q1.put(None); q1.task_done(); break
            q2.put(x * 2.0)
            q1.task_done()

    def aggregator():
        while True:
            y = q2.get()
            if y is None:
                q2.put(None); q2.task_done(); break
            with result["lock"]:
                result["sum"] += y; result["cnt"] += 1
            q2.task_done()

    t0 = time.perf_counter()
    threads = (
        [threading.Thread(target=producer,  daemon=True) for _ in range(n_prod)] +
        [threading.Thread(target=processor, daemon=True) for _ in range(n_proc)] +
        [threading.Thread(target=aggregator, daemon=True) for _ in range(n_agg)]
    )
    [t.start() for t in threads]
    for q in (q1, q2): q.join()
    elapsed = time.perf_counter() - t0
    return elapsed, result["cnt"], result["sum"] / result["cnt"]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["pc", "pipeline"], default="pc")
    ap.add_argument("--prod",  type=int, default=2)
    ap.add_argument("--cons",  type=int, default=2)
    ap.add_argument("--proc",  type=int, default=2, help="processor threads for pipeline")
    ap.add_argument("--agg",   type=int, default=1, help="aggregator threads for pipeline")
    ap.add_argument("--qsize", type=int, default=100)
    ap.add_argument("--items", type=int, default=100000)
    args = ap.parse_args()

    if args.mode == "pc":
        t, n, avg = run_pc(args.prod, args.cons, args.qsize, args.items)
        print(f"[PC]  {n} елементів  |  {t:.2f} c  |  {t/n*1e6:.1f} мкс/ел  |  avg={avg:.5f}")
    else:
        t, n, avg = run_pipeline(args.prod, args.proc, args.agg, args.qsize, args.items)
        print(f"[Pipeline]  {n} елементів  |  {t:.2f} c  |  {t/n*1e6:.1f} мкс/ел  |  avg={avg:.5f}")

if __name__ == "__main__":
    main()
