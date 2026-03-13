
import sys

DB_FILE = "data.db"


class LinkedListIndex:
    class _Node:
        def __init__(self, key, value):
            self.key = key
            self.value = value
            self.next = None

    def __init__(self):
        self.head = None

    def get(self, key):
        node = self.head
        while node:
            if node.key == key:
                return node.value
            node = node.next
        return None

    def set(self, key, value):
        node = self.head
        while node:
            if node.key == key:
                node.value = value
                return
            node = node.next
        new_node = self._Node(key, value)
        new_node.next = self.head
        self.head = new_node


def append_to_log(key, value, db_path=None):
    if db_path is None:
        db_path = DB_FILE
    with open(db_path, "a") as f:
        f.write(f"{key}\t{value}\n")


def load_from_log(idx, db_path=None):
    if db_path is None:
        db_path = DB_FILE
    try:
        with open(db_path, "r") as f:
            for line in f:
                line = line.rstrip("\n")
                if not line:
                    continue
                key, _, value = line.partition("\t")
                idx.set(key, value)
    except FileNotFoundError:
        pass


def parse_and_dispatch(command_str, idx):
    parts = command_str.split()
    if not parts:
        return "ERROR: empty command"
    cmd = parts[0].upper()
    if cmd == "SET":
        if len(parts) < 3:
            return "ERROR: SET requires a key and value"
        key = parts[1]
        value = " ".join(parts[2:])
        append_to_log(key, value)
        idx.set(key, value)
        return "OK"
    elif cmd == "GET":
        if len(parts) < 2:
            return "ERROR: GET requires a key"
        key = parts[1]
        value = idx.get(key)
        return value if value is not None else "NULL"
    elif cmd == "EXIT":
        return None
    else:
        return f"ERROR: unknown command {cmd}"


def main():
    idx = LinkedListIndex()
    load_from_log(idx)
    print("kvstore ready. Commands: SET <key> <value>, GET <key>, EXIT", flush=True)
    for line in sys.stdin:
        line = line.rstrip("\n")
        result = parse_and_dispatch(line, idx)
        if result is None:
            break
        print(result, flush=True)


if __name__ == "__main__":
    main()
