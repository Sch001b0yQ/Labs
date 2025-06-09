import argparse
import queue
import threading
import time
from collections import defaultdict
from pathlib import Path

from PIL import Image, ImageFilter


def load_image(path: Path):
    return path.name, Image.open(path)


def resize_image(data, size=(256, 256)):
    name, img = data
    return name, img.resize(size)


def blur_image(data, radius=1):
    name, img = data
    return name, img.filter(ImageFilter.GaussianBlur(radius=radius))


def save_image_factory(out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)

    def _save(data):
        name, img = data
        img.save(out_dir / name)

    return _save


class Stage(threading.Thread):
    def __init__(self, *, in_q, out_q, func, name: str, stats):
        super().__init__(daemon=True, name=name)
        self.in_q = in_q
        self.out_q = out_q
        self.func = func
        self.stats = stats[name]

    def run(self):
        while True:
            item = self.in_q.get()
            if item is None:
                if self.out_q is not None:
                    self.out_q.put(None)
                self.in_q.task_done()
                break
            t0 = time.perf_counter()
            result = self.func(item)
            self.stats.append(time.perf_counter() - t0)
            if self.out_q is not None:
                self.out_q.put(result)
            self.in_q.task_done()


class Source(threading.Thread):
    def __init__(self, *, img_paths, out_q):
        super().__init__(daemon=True, name="Source")
        self.img_paths = img_paths
        self.out_q = out_q

    def run(self):
        for path in self.img_paths:
            self.out_q.put(path)
        self.out_q.put(None)


def main(in_dir: Path, out_dir: Path, queue_size: int = 8):
    q_load = queue.Queue(queue_size)
    q_resize = queue.Queue(queue_size)
    q_blur = queue.Queue(queue_size)
    q_save = queue.Queue(queue_size)

    stats = defaultdict(list)

    valid_suffixes = (".png", ".jpg", ".jpeg")
    img_paths = sorted(p for p in in_dir.rglob("*") if p.suffix.lower() in valid_suffixes)
    if not img_paths:
        print(f"У теці «{in_dir}» немає відповідних файлів")
        return

    stages = [
        Source(img_paths=img_paths, out_q=q_load),
        Stage(in_q=q_load, out_q=q_resize, func=load_image, name="Load", stats=stats),
        Stage(in_q=q_resize, out_q=q_blur, func=resize_image, name="Resize", stats=stats),
        Stage(in_q=q_blur, out_q=q_save, func=blur_image, name="Blur", stats=stats),
        Stage(in_q=q_save, out_q=None, func=save_image_factory(out_dir), name="Save", stats=stats),
    ]

    t0 = time.perf_counter()
    for s in stages:
        s.start()

    for q in (q_load, q_resize, q_blur, q_save):
        q.join()
    for s in stages:
        s.join()

    elapsed = time.perf_counter() - t0
    total = len(stats["Save"])

    print(f"Перероблено файлів: {total}")
    if total:
        print(f"Загальний час: {elapsed:.2f} с ({elapsed / total:.3f} с/файл)")
    for stage, t_list in stats.items():
        if t_list:
            avg_t = sum(t_list) / len(t_list)
            print(f"{stage:<6}: avg {avg_t:.4f} с, max {max(t_list):.4f} с")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("in_dir", nargs="?", type=Path, default=Path(r"E:\img"))
    parser.add_argument("out_dir", nargs="?", type=Path, default=Path(r"E:\img\out"))
    args = parser.parse_args()
    main(args.in_dir, args.out_dir)
