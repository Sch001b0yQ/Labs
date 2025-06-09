import argparse, time, re, math
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

word_re = re.compile(r"[A-Za-zÀ-ÖØ-öø-ÿҐЄІЇА-Яа-яʼ’'-]+")

def slice_lines(lines, n):
    k = math.ceil(len(lines) / n)
    for i in range(0, len(lines), k):
        yield lines[i : i + k]

def map_count(chunk):
    c = Counter()
    for line in chunk:
        c.update(w.lower() for w in word_re.findall(line))
    return c

def reduce_counts(partials):
    total = Counter()
    for c in partials:
        total.update(c)
    return total

def wordcount(path: Path, n_threads: int):
    lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    chunks = list(slice_lines(lines, n_threads))
    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=n_threads) as exe:
        partials = list(exe.map(map_count, chunks))
    total = reduce_counts(partials)
    elapsed = time.perf_counter() - t0
    return elapsed, total

def main():
    ap = argparse.ArgumentParser(description="MapReduce-style підрахунок частоти слів")
    ap.add_argument("file", type=Path)
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--bench", type=str,
                    help="перелік потоків через кому для серії вимірів, напр. 1,2,4,8")
    args = ap.parse_args()

    if args.bench:
        print(f"Бенчмарк файлу {args.file} ({args.file.stat().st_size/1e6:.2f} MB)")
        for n in map(int, args.bench.split(",")):
            t, _ = wordcount(args.file, n)
            print(f"{n:>2} поток(и): {t:.3f} с")
    else:
        t, cnt = wordcount(args.file, args.threads)
        most = cnt.most_common(20)
        print(f"Час: {t:.3f} с  |  усього різних слів: {len(cnt)}")
        for w, c in most:
            print(f"{w:<15} {c}")

if __name__ == "__main__":
    main()
