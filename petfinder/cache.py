import abc
import atexit
import json
import os
import shutil
import tempfile
import time
from hashlib import md5
from typing import Optional, Callable, Hashable, Dict, AnyStr, Union

from httpx import Request, Response

SaveConditionCallbackType = Callable[[Request, Response], bool]
KeepForeverCallbackType = Callable[[Request], bool]


def default_keep_forever_behavior(request: Request) -> bool:
    """
    Default behavior for what responses should be cached forever.
    API resources that never really change are kept forever.
    """
    if request.url.path.startswith("/v2/types"):
        return True
    return False


class ResponseCache(abc.ABC):
    time_to_live: int
    save_condition: Optional[SaveConditionCallbackType]
    keep_forever: Optional[KeepForeverCallbackType]

    def __init__(
        self,
        time_to_live: int = 3600,
        save_condition: SaveConditionCallbackType = None,
        keep_forever: KeepForeverCallbackType = default_keep_forever_behavior,
    ):
        self.time_to_live = time_to_live
        self.save_condition = save_condition
        self.keep_forever = keep_forever

    def _cached_value_expired(self, request: Request) -> bool:
        if self.keep_forever is not None and self.keep_forever(request):
            return False
        key = self.as_key(request)
        return (time.time() - self.get_last_cached_at(key)) > self.time_to_live

    def _response_should_be_cached(self, request: Request, response: Response) -> bool:
        if self.save_condition is None:
            return True
        return self.save_condition(request, response)

    def has_response(self, request: Request) -> bool:
        key = self.as_key(request)
        if key not in self:
            return False
        elif self._cached_value_expired(request):
            del self[key]
            return False
        else:
            return True

    def get_response(self, request: Request) -> Union[Dict, None]:
        key = self.as_key(request)
        return self.deserialize_response_data(self[key])

    def save_response(self, request: Request, response: Response) -> None:
        if self._response_should_be_cached(request, response):
            self[self.as_key(request)] = self.serialize_response_data(response)

    @abc.abstractmethod
    def __del__(self):
        ...

    @abc.abstractmethod
    def __getitem__(self, key):
        ...

    @abc.abstractmethod
    def __setitem__(self, key, value):
        ...

    @abc.abstractmethod
    def __delitem__(self, key):
        ...

    @abc.abstractmethod
    def __contains__(self, key):
        ...

    @abc.abstractmethod
    def as_key(self, request: Request) -> Hashable:
        ...

    @abc.abstractmethod
    def get_last_cached_at(self, key: Hashable) -> int:
        ...

    @abc.abstractmethod
    def serialize_response_data(self, response: Response) -> AnyStr:
        ...

    @abc.abstractmethod
    def deserialize_response_data(self, serialized: AnyStr) -> Dict:
        ...


class FileCache(ResponseCache):
    directory: str

    def __init__(
        self,
        directory: str,
        time_to_live: int = 3600,
        save_condition: SaveConditionCallbackType = None,
        keep_forever: KeepForeverCallbackType = default_keep_forever_behavior,
    ):
        super().__init__(time_to_live, save_condition, keep_forever)
        self.directory = directory
        os.makedirs(directory, exist_ok=True)

    def __del__(self):
        pass

    def __getitem__(self, key):
        with open(key) as f:
            return f.read()

    def __setitem__(self, key, value):
        with open(key, "w") as f:
            f.write(value)

    def __delitem__(self, key):
        if os.path.exists(key):
            os.remove(key)

    def __contains__(self, key):
        return os.path.exists(key)

    def as_key(self, request: Request) -> Hashable:
        return os.path.join(
            self.directory, f"{md5(request.url.raw_path).hexdigest()}.json"
        )

    def serialize_response_data(self, response: Response) -> AnyStr:
        return response.text

    def deserialize_response_data(self, serialized: AnyStr) -> Dict:
        return json.loads(serialized)

    def get_last_cached_at(self, key: str) -> int:
        return int(os.path.getmtime(key))


class TempFileCache(FileCache):
    def __init__(
        self,
        time_to_live: int = 3600,
        save_condition: SaveConditionCallbackType = None,
        keep_forever: KeepForeverCallbackType = default_keep_forever_behavior,
    ):
        directory = tempfile.mkdtemp()
        super().__init__(directory, time_to_live, save_condition, keep_forever)
        atexit.register(shutil.rmtree, self.directory)
