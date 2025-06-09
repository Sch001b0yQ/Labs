from dataclasses import dataclass
from threading import Thread, Lock
from time import sleep
import uuid

@dataclass(frozen=True)
class Event:
    account_id: str
    version: int
    type: str
    amount: int

class EventStore:
    def __init__(self):
        self._events: list[Event] = []
        self._lock = Lock()

    def append(self, event: Event):
        with self._lock:
            self._events.append(event)

    def load(self, account_id: str):
        with self._lock:
            return [e for e in self._events if e.account_id == account_id]

store = EventStore()

class Account:
    def __init__(self, events: list[Event]):
        self.balance = 0
        self.version = 0
        for e in sorted(events, key=lambda e: e.version):
            self.apply(e)

    def apply(self, event: Event):
        if event.type == "DEPOSIT":
            self.balance += event.amount
        elif event.type == "WITHDRAW":
            self.balance -= event.amount
        self.version = event.version

def create_account() -> str:
    account_id = str(uuid.uuid4())
    store.append(Event(account_id, 1, "OPEN", 0))
    return account_id

def handle_command(account_id: str, cmd_type: str, amount: int = 0):
    while True:
        history = store.load(account_id)
        acc = Account(history)
        next_version = acc.version + 1
        if cmd_type == "DEPOSIT":
            new_event = Event(account_id, next_version, "DEPOSIT", amount)
        elif cmd_type == "WITHDRAW":
            if acc.balance < amount:
                return False
            new_event = Event(account_id, next_version, "WITHDRAW", amount)
        else:
            return False
        # optimistic concurrency — перевірка, чи не додано нових подій
        with store._lock:
            if next_version - 1 == len([e for e in store._events if e.account_id == account_id]):
                store._events.append(new_event)
                return True
        sleep(0.001)

def scenario():
    acc_id = create_account()
    threads: list[Thread] = []
    for _ in range(50):
        t1 = Thread(target=handle_command, args=(acc_id, "DEPOSIT", 10))
        t2 = Thread(target=handle_command, args=(acc_id, "WITHDRAW", 5))
        threads.extend([t1, t2])
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    final_state = Account(store.load(acc_id))
    print("Подій у сховищі:", len(store.load(acc_id)))
    print("Поточний баланс:", final_state.balance)

if __name__ == "__main__":
    scenario()
