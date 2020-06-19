import json
import os
import pathlib

from dataclasses import dataclass
from datetime import datetime
from random import random
from time import sleep
from typing import Callable
from typing import Iterator
from typing import List
from typing import Optional
from typing import Tuple
from typing import TypeVar

from pytyped.json.decoder import AutoJsonDecoder
from pytyped.json.decoder import JsonDecoder
from pytyped.json.encoder import AutoJsonEncoder
from pytyped.json.encoder import JsonEncoder

Key = List[str]  # Each string should only use base64 chars, i.e., `A`-`Z`, `a`-`z`, `0`-`9`, `+`, or `/`
Value = bytes


@dataclass
class DatabaseException(Exception):
    pass


@dataclass
class InvalidKeyException(DatabaseException):
    key: Key


@dataclass
class LockNotObtainedException(DatabaseException):
    key: Key


A = TypeVar("A")


@dataclass
class Lock:
    process_id: int
    created_at: datetime


@dataclass
class RetryStrategy:
    max_retries: int = 0
    initial_backoff_seconds: float = 0.1

    additive_increase: float = 0.0
    multiplicative_increase: float = 0.0

    min_jitter_additive: float = 0.0
    min_jitter_multiplicative: float = 0.0

    max_jitter_additive: float = 0.0
    max_jitter_multiplicative: float = 0.0


_auto_json_encoder: AutoJsonEncoder = AutoJsonEncoder()
_lock_encoder: JsonEncoder[Lock] = _auto_json_encoder.extract(Lock)

_auto_json_decoder: AutoJsonDecoder = AutoJsonDecoder()
_lock_decoder: JsonDecoder[Lock] = _auto_json_decoder.extract(Lock)


@dataclass
class UntypedStore:
    base_dir: str
    base_key: Key
    retry_strategy: RetryStrategy = RetryStrategy()

    @staticmethod
    def _is_base64_char_only(key: Key) -> bool:
        for s in key:
            for c in s:
                if ('A' <= c <= 'Z') or ('a' <= c <= 'z') or ('0' <= c <= '9') or c == '+' or c == '/':
                    continue
                return False
        return True

    def _get_tmp_dir(self) -> str:
        return self.base_dir + 'tmp/'

    def _get_data_dir(self) -> str:
        return self.base_dir + 'data/'

    def _get_path(self, key: Key) -> str:
        full_key: Key = self.base_key + key
        if len(full_key) == 0 or not UntypedStore._is_base64_char_only(full_key):
            raise InvalidKeyException(full_key)
        last_index = len(full_key) - 1
        converted = [s.replace("/", "-") + (".d" if i < last_index else "") for i, s in enumerate(full_key)]
        return self._get_data_dir() + "/".join(converted)

    def _get_data_file_name(self, key: Key) -> str:
        return self._get_path(key) + ".data"

    @staticmethod
    def _read_data(data_file_name: str) -> Optional[Value]:
        try:
            data = pathlib.Path(data_file_name).read_bytes()
            return data
        except FileNotFoundError:
            return None

    @staticmethod
    def _write_data(data_file_name: str, contents: Value) -> None:
        pathlib.Path(data_file_name).write_bytes(contents)

    def get(self, key: Key) -> Optional[Value]:
        return UntypedStore._read_data(self._get_data_file_name(key))

    # raises an exception if `key` is locked.
    def put(self, key: Key, value: Value, retry_strategy: Optional[RetryStrategy] = None) -> None:
        def write_to_file(data_file_name: str) -> None:
            UntypedStore._write_data(data_file_name, value)

        if retry_strategy is None:
            retry_strategy = self.retry_strategy
        self._lock(key, write_to_file, retry_strategy)

    # The scan method does not lock anything and it is only guaranteed to work as expected if no other process changes
    # the state of the database while a scan is going on.
    # If the state of the database changes in between, the scanner method may return key-value pairs that no longer
    # exist or it may skips key-value pairs that are generated and/or updated in the meantime.
    # The returned keys are partial meaning that the base_key of the current instance of UntypedStore is removed.
    def prefix_scan(self, key_prefix: Key) -> Iterator[Tuple[Key, Value]]:
        base_dir: str
        if len(self.base_key) <= 0:
            base_dir = self.base_dir
        else:
            base_dir = self._get_path([]) + ".d/"
        prefix_dir = self._get_path(key_prefix) + ".d/"
        for dir_path, _, file_names in os.walk(prefix_dir):
            if dir_path.startswith(base_dir):
                for file_name in file_names:
                    if file_name.endswith(".data"):
                        full_name = os.path.join(dir_path, file_name)
                        remaining_name = full_name[len(base_dir): -5]  # Removes base_dir prefix and `.lock` postfix.
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
                            value = self.get(decoded_key)
                            if value is not None:  # Check for None as data might have been removed since scan started
                                yield decoded_key, value

    # raises an exception if `key` is locked.
    # returns false if the value did not match the expected value
    # returns true if everything went well.
    def compare_and_put(
        self,
        key: Key,
        expected: Optional[Value],
        new: Value,
        retry_strategy: Optional[RetryStrategy] = None
    ) -> bool:
        def compare_contents(data_file_name: str) -> bool:
            existing_data = self._read_data(data_file_name)
            if existing_data == expected:
                UntypedStore._write_data(data_file_name, new)
                return True
            return False

        if retry_strategy is None:
            retry_strategy = self.retry_strategy
        return self._lock(key, compare_contents, retry_strategy)

    # raises an exception if `key` is locked.
    # f is the function that is called with only parameter: the file that is guaranteed to be locked.
    # the file itself is not guaranteed to exist but the directory possibly containing that file is guaranteed to exist.
    def _lock(self, key: Key, f: Callable[[str], A], retry_strategy: RetryStrategy) -> A:
        lock = Lock(
            process_id=os.getpid(),
            created_at=datetime.utcnow()
        )

        base_file_name = self._get_path(key)
        lock_file_name = base_file_name + ".lock"

        retry_count: int = retry_strategy.max_retries
        backoff: float = retry_strategy.initial_backoff_seconds
        while retry_count >= 0:
            try:
                base_dir = pathlib.Path(base_file_name).parts[:-1]
                pathlib.Path("/".join(base_dir)).mkdir(parents=True, exist_ok=True)

                flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
                file_handle = os.open(lock_file_name, flags)
            except FileExistsError:
                if retry_count > 0:
                    min_jitter = retry_strategy.min_jitter_multiplicative * backoff + retry_strategy.min_jitter_additive
                    max_jitter = retry_strategy.max_jitter_multiplicative * backoff + retry_strategy.max_jitter_additive
                    jitter = min_jitter + random() * (max_jitter - min_jitter)
                    sleep(backoff + jitter)
                    backoff += retry_strategy.multiplicative_increase * backoff + retry_strategy.additive_increase

                retry_count -= 1
            else:  # No exception, so the file must have been created successfully.
                try:
                    os.close(file_handle)

                    lock_file = open(lock_file_name, 'w')
                    lock_file.write(json.dumps(_lock_encoder.write(lock)))
                    lock_file.flush()
                    os.fsync(lock_file.fileno())
                    lock_file.close()

                    return f(base_file_name + ".data")
                finally:
                    os.remove(lock_file_name)

        raise LockNotObtainedException(key)

    # raises an exception if `key` is locked.
    def lock_and_run(
        self,
        key: Key,
        f: Callable[[Optional[Value]], A],
        retry_strategy: Optional[RetryStrategy] = None
    ) -> A:
        def run_f(data_file_name: str) -> A:
            data = self._read_data(data_file_name)
            return f(data)

        if retry_strategy is None:
            retry_strategy = self.retry_strategy
        return self._lock(key, run_f, retry_strategy)

    # f is expected to return a potentially new value (to update key) plus the result of function
    def lock_and_transform(
        self,
        key: Key,
        f: Callable[[Optional[Value]], Tuple[Optional[Value], A]],
        retry_strategy: Optional[RetryStrategy] = None
    ) -> A:
        def run_f(data_file_name: str) -> A:
            data = self._read_data(data_file_name)
            (maybe_new_value, result) = f(data)
            if maybe_new_value is not None:
                self._write_data(data_file_name, maybe_new_value)
            return result

        if retry_strategy is None:
            retry_strategy = self.retry_strategy
        return self._lock(key, run_f, retry_strategy)

    def get_descendant_store(self, descendant_key: Key) -> "UntypedStore":
        if len(descendant_key) <= 0:
            return self
        return UntypedStore(self.base_dir, self.base_key + descendant_key)

    def get_ancestor_store(self, level: int = 1) -> "UntypedStore":
        if level <= 0:
            return self
        if level >= len(self.base_key):
            level = len(self.base_key)
        return UntypedStore(self.base_dir, self.base_key[:-level])
