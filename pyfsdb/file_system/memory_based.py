import os
import uuid
from dataclasses import dataclass
from dataclasses import field
from typing import Callable
from typing import Dict
from typing import Iterator
from typing import List
from typing import Optional
from typing import Tuple
from typing import TypeVar

from pytyped.macros.boxed import Boxed

from pyfsdb.file_system.base import FileSystem

A = TypeVar("A")
Key = List[str]
Value = bytes


@dataclass
class MemoryBasedFS(FileSystem):
    values: Dict[str, bytes] = field(default_factory=dict)

    @staticmethod
    def _is_valid(key: Key) -> bool:
        for s in key:
            if "." in s:
                return False
        return True

    @staticmethod
    def _flatten_key(key: Key) -> Optional[str]:
        if len(key) == 0 or not MemoryBasedFS._is_valid(key):
            return None
        return ".".join(key)

    def read_data(self, key: Key) -> Optional[Value]:
        flattened = MemoryBasedFS._flatten_key(key)
        if flattened is None:
            return None
        return self.values.get(flattened + ".data")

    def write_data(self, key: Key, contents: Value) -> None:
        flattened = MemoryBasedFS._flatten_key(key)
        if flattened is None:
            return
        self.values[flattened + ".data"] = contents

    def prefix_scan(self, key_prefix: Key) -> Iterator[Tuple[Key, Value]]:
        flattened = MemoryBasedFS._flatten_key(key_prefix)
        if flattened is None:
            return

        base = flattened + "."
        kvs = [(k[:-5], v) for k, v in self.values.items() if k.startswith(base) and k.endswith(".data")]
        for k, v in kvs:
            yield k.split("."), v

    def lock_and_run(self, key: Key, f: Callable[[], A]) -> Optional[Boxed[A]]:
        flattened = MemoryBasedFS._flatten_key(key)
        if flattened is None:
            return None

        lock = flattened + ".lock"
        id = str(os.getpid()).encode()
        lock_owner = self.values.setdefault(lock, id)
        if lock_owner == id:
            try:
                return Boxed(f())
            finally:
                del self.values[lock]
        return None
