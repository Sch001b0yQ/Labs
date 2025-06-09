import threading, queue, time, random

def service_call(x: int) -> int:
    time.sleep(random.uniform(0.1, 0.5))
    return x * x

class ActiveObject:
    def __init__(self):
        self._requests = queue.Queue()
        self._worker = threading.Thread(target=self._run, daemon=True)
        self._worker.start()

    def _run(self):
        while True:
            func, args, kwargs, future = self._requests.get()
            try:
                future["result"] = func(*args, **kwargs)
            finally:
                future["done"] = True
                self._requests.task_done()

    def submit(self, func, *args, **kwargs):
        future = {"done": False, "result": None}
        self._requests.put((func, args, kwargs, future))
        return future

def run_sync(n=20):
    start = time.perf_counter()
    for i in range(n):
        service_call(i)
    return time.perf_counter() - start

def run_active_object(n=20):
    ao = ActiveObject()
    futures = [ao.submit(service_call, i) for i in range(n)]
    start = time.perf_counter()
    for fut in futures:
        while not fut["done"]:
            time.sleep(0.005)
    return time.perf_counter() - start

if __name__ == "__main__":
    NUM = 20
    t_sync = run_sync(NUM)
    t_ao = run_active_object(NUM)
    print(f"Синхронно  : {t_sync:.3f} c")
    print(f"Active-Obj : {t_ao:.3f} c")
