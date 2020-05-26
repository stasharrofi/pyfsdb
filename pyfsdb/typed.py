import json

from dataclasses import dataclass
from typing import Callable
from typing import Generator
from typing import Generic
from typing import Optional
from typing import Tuple
from typing import TypeVar

from pytyped.macros.boxed import Boxed
from pytyped.json.decoder import JsonDecoder
from pytyped.json.encoder import JsonEncoder

from pyfsdb.untyped import Key
from pyfsdb.untyped import UntypedStore
from pyfsdb.untyped import Value

A = TypeVar("A")
T = TypeVar("T")


@dataclass
class GetResult(Generic[T]):
    # The exact encoded bytes will be needed for compare_and_put because the encoder might not be stable.
    exact: Value
    value: T


@dataclass
class TypedStore(Generic[T]):
    underlying: UntypedStore
    t_decoder: JsonDecoder[T]
    t_encoder: JsonEncoder[T]

    def _decode(self, value: Value) -> T:
        return self.t_decoder.read(json.loads(value.decode(encoding="utf-8")))

    def _encode(self, value: T) -> Value:
        return json.dumps(self.t_encoder.write(value)).encode(encoding="utf-8")

    def get(self, key: Key) -> Optional[GetResult[T]]:
        maybe_bytes = self.underlying.get(key)
        if maybe_bytes is None:
            return None

        return GetResult(maybe_bytes, self._decode(maybe_bytes))

    # raises an exception if `key` is locked.
    def put(self, key: Key, value: T) -> None:
        self.underlying.put(key, self._encode(value))

    # The scan method does not lock anything and it is only guaranteed to work as expected if no other process changes
    # the state of the database while a scan is going on.
    # If the state of the database changes in between, the scanner method may return key-value pairs that no longer
    # exist or it may skips key-value pairs that are generated and/or updated in the meantime.
    # The returned keys are partial meaning that the base_key of the current instance of UntypedStore is removed.
    def prefix_scan(self, key_prefix: Key) -> Generator[Tuple[Key, T]]:
        for key, value in self.prefix_scan(key_prefix):
            yield key, self._decode(value)

    # raises an exception if `key` is locked.
    # returns false if the value did not match the expected value
    # returns true if everything went well.
    def compare_and_put(self, key: Key, expected: Optional[Value], new: T) -> bool:
        return self.underlying.compare_and_put(key, expected, self._encode(new))

    # raises an exception if `key` is locked.
    def lock_and_run(self, key: Key, f: Callable[[Optional[GetResult[T]]], A]) -> A:
        def run_f(value: Optional[Value]) -> A:
            if value is None:
                return f(None)
            return f(GetResult(value, self._decode(value)))

        return self.underlying.lock_and_run(key, run_f)

    def lock_and_transform(self, key: Key, f: Callable[[Optional[GetResult[T]]], Tuple[Optional[Boxed[T]], A]]) -> A:
        def run_f(value: Optional[Value]) -> Tuple[Optional[Value], A]:
            if value is None:
                return f(None)
            (maybe_new_data, result) = f(GetResult(value, self._decode(value)))
            encoded_new_data: Optional[Value] = None
            if maybe_new_data is not None:
                encoded_new_data = self._encode(maybe_new_data.t)
            return encoded_new_data, result

        return self.underlying.lock_and_transform(key, run_f)

    def get_descendant_store(self, descendant_key: Key) -> "TypedStore"[T]:
        return TypedStore(self.underlying.get_descendant_store(descendant_key), self.t_decoder, self.t_encoder)

    def get_ancestor_store(self, level: int = 1) -> "TypedStore"[T]:
        return TypedStore(self.underlying.get_ancestor_store(level), self.t_decoder, self.t_encoder)
