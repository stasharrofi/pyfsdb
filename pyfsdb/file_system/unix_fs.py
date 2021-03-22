import json
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Callable
from typing import Iterator
from typing import List
from typing import Optional
from typing import Tuple
from typing import TypeVar

import pathlib
from pytyped.json.decoder import AutoJsonDecoder
from pytyped.json.decoder import JsonDecoder
from pytyped.json.encoder import AutoJsonEncoder
from pytyped.json.encoder import JsonEncoder
from pytyped.macros.boxed import Boxed

from pyfsdb.file_system.base import FileSystem


@dataclass
class Lock:
    process_id: int
    created_at: datetime


_auto_json_encoder: AutoJsonEncoder = AutoJsonEncoder()
_lock_encoder: JsonEncoder[Lock] = _auto_json_encoder.extract(Lock)

_auto_json_decoder: AutoJsonDecoder = AutoJsonDecoder()
_lock_decoder: JsonDecoder[Lock] = _auto_json_decoder.extract(Lock)


A = TypeVar("A")
Key = List[str]  # Each string should only use base64 chars, i.e., `A`-`Z`, `a`-`z`, `0`-`9`, `+`, or `/`
Value = bytes


@dataclass
class UnixLikeFS(FileSystem):
    base_dir: str

    @staticmethod
    def _is_base64_char_only(key: Key) -> bool:
        for s in key:
            for c in s:
                if ('A' <= c <= 'Z') or ('a' <= c <= 'z') or ('0' <= c <= '9') or c == '+' or c == '/':
                    continue
                return False
        return True

    def get_data_dir(self) -> str:
        return self.base_dir + 'data/'

    def flatten_key(self, key: Key) -> Optional[str]:
        if len(key) == 0 or not UnixLikeFS._is_base64_char_only(key):
            return None
        last_index = len(key) - 1
        converted = [s.replace("/", "-") + (".d" if i < last_index else "") for i, s in enumerate(key)]
        return self.get_data_dir() + "/".join(converted)

    def read_data(self, key: Key) -> Optional[Value]:
        try:
            flattened_key = self.flatten_key(key)
            if flattened_key is None:
                return None
            data = pathlib.Path(flattened_key).read_bytes()
            return data
        except FileNotFoundError:
            return None

    def write_data(self, key: Key, contents: Value) -> None:
        flattened_key = self.flatten_key(key)
        if flattened_key is None:
            return None
        pathlib.Path(flattened_key).write_bytes(contents)

    def prefix_scan(self, key_prefix: Key) -> Iterator[Tuple[Key, Value]]:
        base_dir = self.base_dir
        flattened_key = self.flatten_key(key_prefix)
        if flattened_key is None:
            return
        prefix_dir = flattened_key + ".d/"
        for dir_path, _, file_names in os.walk(prefix_dir):
            if dir_path.startswith(base_dir):
                for file_name in file_names:
                    if file_name.endswith(".data"):
                        full_name = os.path.join(dir_path, file_name)
                        remaining_name = full_name[len(base_dir): -5]  # Removes base_dir prefix and `.data` postfix.
                        parts = pathlib.Path(remaining_name).parts
                        decoded_key: Optional[Key] = []
                        for part in parts:
                            if not part.endswith(".d"):
                                decoded_key = None
                            if decoded_key is None:
                                break
                            deconverted_part = part[:-2].replace("-", "/")  # Remove `.d` in the end and back to base64
                            decoded_key.append(deconverted_part)

                        if decoded_key is not None:
                            value = self.read_data(decoded_key)
                            if value is not None:  # Check for None as data might have been removed since scan started
                                yield decoded_key, value

    def lock_and_run(self, key: Key, f: Callable[[], A]) -> Optional[Boxed[A]]:
        lock = Lock(
            process_id=os.getpid(),
            created_at=datetime.utcnow()
        )

        base_file_name = self.flatten_key(key)
        if base_file_name is None:
            return None
        lock_file_name = base_file_name + ".lock"

        try:
            base_dir = pathlib.Path(base_file_name).parts[:-1]
            pathlib.Path("/".join(base_dir)).mkdir(parents=True, exist_ok=True)

            flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
            file_handle = os.open(lock_file_name, flags)
        except FileExistsError:
            return None
        else:  # No exception, so the file must have been created successfully.
            try:
                os.close(file_handle)

                lock_file = open(lock_file_name, 'w')
                lock_file.write(json.dumps(_lock_encoder.write(lock)))
                lock_file.flush()
                os.fsync(lock_file.fileno())
                lock_file.close()

                return Boxed(f())
            finally:
                os.remove(lock_file_name)
