import abc
import pickle
import time
from pathlib import Path
from typing import Callable, Union, TypedDict, TypeVar, NamedTuple

import appdirs
from httpx import Request, Response, Headers, URL

T = TypeVar("T")


class CachedRequest(NamedTuple):
    url: URL
    method: str
    content: bytes


class CachedResponse(TypedDict):
    headers: Headers
    content: bytes


TimeToLive = Union[None, int]
TimeToLiveCallback = Callable[[CachedRequest], TimeToLive]


def default_ttl_callback(request: CachedRequest) -> TimeToLive:
    """
    Default behavior for determining how long to cache responses.
    API resources that never really change are kept for a week,
    everything else is kept for a day.
    """
    if request.url.path.startswith("/v2/types"):
        return 604800
    return 86400


def data_directory() -> str:
    """
    Returns the default directory that should be used for storing cached data.
    """
    path = Path(appdirs.user_cache_dir("petfinder-sdk"))
    if not path.exists():
        path.mkdir(exist_ok=True, parents=True)
    return path.as_posix()


class RequestsCache:
    time_to_live: TimeToLiveCallback

    def __init__(
        self, time_to_live: TimeToLiveCallback = default_ttl_callback,
    ):
        self.time_to_live = time_to_live

    @abc.abstractmethod
    def open(self):
        ...

    @abc.abstractmethod
    def is_open(self) -> bool:
        ...

    @abc.abstractmethod
    def close(self):
        ...

    @abc.abstractmethod
    def __del__(self):
        ...

    @abc.abstractmethod
    def __getitem__(self, key: bytes) -> bytes:
        ...

    @abc.abstractmethod
    def __setitem__(self, key: bytes, value: bytes) -> None:
        ...

    @abc.abstractmethod
    def __delitem__(self, key: bytes) -> None:
        ...

    @abc.abstractmethod
    def __contains__(self, key: bytes) -> bool:
        ...

    @abc.abstractmethod
    def get_timestamp(self, key: bytes) -> int:
        ...

    def create_key(self, request: Request) -> bytes:
        """
        Converts a request into the cache key
        """
        return pickle.dumps(CachedRequest(request.url, request.method, request.read()))

    def deserialize_key(self, key: bytes) -> CachedRequest:
        """
        Converts a key back into the cached request representation.
        """
        return pickle.loads(key)

    def serialize_response(self, response: Response) -> bytes:
        """
        Converts a response to a bytes representation that can be saved anywhere.
        """
        return pickle.dumps(
            CachedResponse(headers=response.headers, content=response.content)
        )

    def deserialize_response(self, data: bytes) -> CachedResponse:
        """
        Convert the bytes representation from serialize_response back into a CachedResponse.
        """
        return pickle.loads(data)

    def is_expired(self, key: bytes) -> bool:
        """
        Return true if a cached response has expired.
        """
        ttl = self.time_to_live(self.deserialize_key(key)) or 0
        return (time.time() - self.get_timestamp(key)) > ttl

    def has(self, request: Request) -> bool:
        """
        Returns true if a request exists in the cache.
        """
        key = self.create_key(request)
        if key not in self:
            return False
        elif self.is_expired(key):
            del self[key]
            return False
        return True

    def get(self, request: Request) -> CachedResponse:
        """
        Retrieve a cached response for a request.
        This assumes you have already checked if the request exists in the cache.
        """
        key = self.create_key(request)
        data = self[key]
        return self.deserialize_response(data)

    def save(self, request: Request, response: Response) -> None:
        """
        Saves a request/response pair in the cache IF they should actually be kept.
        """
        key = self.create_key(request)
        if self.time_to_live(self.deserialize_key(key)):
            data = self.serialize_response(response)
            self[key] = data
