import asyncio
import json
from types import TracebackType
from typing import Optional, TypeVar, Any, Union, Type, Iterable, Dict

import httpx

from petfinder.animals import AnimalsQuery
from petfinder.auth import TokenAuth
from petfinder.enums import Category, Age
from petfinder.caching import RequestsCache, CachedResponse
from petfinder.query import Query

T = TypeVar("T")


class PetfinderClient:
    animals: AnimalsQuery
    cache: Optional[RequestsCache]
    http_kwargs: Dict[str, Any]

    def __init__(
        self,
        *,
        api_key: str,
        secret: str,
        cache: Optional[RequestsCache] = None,
        base_url: str = "https://api.petfinder.com/v2",
        **httpx_client_kwargs,
    ) -> None:
        self.cache = cache
        self.http_kwargs = {
            "auth": TokenAuth(
                secret=secret, api_key=api_key, token_url=f"{base_url}/oauth2/token",
            ),
            "base_url": base_url,
            **httpx_client_kwargs,
        }
        self.animals = AnimalsQuery(
            path="animals",
            executor=self.fetch,
            async_executor=self.async_fetch,
            async_batch_executor=self.async_fetch_many,
        )

    @property
    def dogs(self) -> AnimalsQuery:
        return self.animals._chain(type=Category.dog)

    @property
    def puppies(self) -> AnimalsQuery:
        return self.animals._chain(type=Category.dog, age=[Age.baby])

    @property
    def cats(self) -> AnimalsQuery:
        return self.animals._chain(type=Category.cat)

    @property
    def kittens(self) -> AnimalsQuery:
        return self.animals._chain(type=Category.cat, age=[Age.baby])

    @property
    def small_furry(self) -> AnimalsQuery:
        return self.animals._chain(type=Category.small_furry)

    @property
    def birds(self) -> AnimalsQuery:
        return self.animals._chain(type=Category.bird)

    @property
    def rabbits(self) -> AnimalsQuery:
        return self.animals._chain(type=Category.rabbit)

    @property
    def horses(self) -> AnimalsQuery:
        return self.animals._chain(type=Category.horse)

    @property
    def barnyard(self) -> AnimalsQuery:
        return self.animals._chain(type=Category.barnyard)

    @property
    def scales_fins_other(self) -> AnimalsQuery:
        return self.animals._chain(type=Category.scales_fins_other)

    def fetch(self, query: Query[T], client: httpx.Client = None) -> T:
        """
        Standard, synchronous execution of a single query.
        """
        with HttpClient(client, self.http_kwargs, self.cache) as c:
            request = self.build_request(query, c)
            if self.cache and self.cache.has(request):
                return self.process_cached_response(self.cache.get(request))
            return self.process_response(request, c.send(request))

    async def async_fetch(self, query: Query[T], client: httpx.AsyncClient = None) -> T:
        """
        Asynchronous execution of a single query.
        """
        async with HttpClient(client, self.http_kwargs, self.cache) as c:
            request = self.build_request(query, c)
            if self.cache and self.cache.has(request):
                return self.process_cached_response(self.cache.get(request))
            return self.process_response(request, await c.send(request))

    async def async_fetch_many(
        self, queries: Iterable[Query[T]], client: httpx.AsyncClient = None
    ) -> Iterable[T]:
        """
        Asynchronous execution of a batch of queries concurrently, returning the results as a list.
        """
        async with HttpClient(client, self.http_kwargs, self.cache) as c:
            return await asyncio.gather(*[self.async_fetch(q, c) for q in queries])

    def build_request(
        self, query: Query, client: Union[httpx.Client, httpx.AsyncClient]
    ) -> httpx.Request:
        """
        Build and return an httpx Request for the given query.

        If a pydantic model for parsing/validating query parameters has been defined,
        we will now use it to parse the raw query params into a finalized form.
        """
        params = (
            query.params
            if query.params_class is None
            else query.params_class(__query__=query, **query.params).dict()
        )
        return client.build_request("GET", query.path, params=params)

    def process_response(self, request: httpx.Request, response: httpx.Response) -> Any:
        """
        Check for errors, cache the response data (if appropriate), and then return it.
        """
        response.raise_for_status()
        if self.cache is not None:
            self.cache.save(request, response)
        return response.json()

    def process_cached_response(self, cached_response: CachedResponse) -> Any:
        """
        Process a cached response, transforming it into the expected output.
        """
        return json.loads(cached_response["content"].decode("utf-8"))


class HttpClient:
    """
    A context manager which allows for efficient reuse of existing client and cache connections.
    """

    client: Union[httpx.Client, httpx.AsyncClient, None]
    kwargs: Dict[str, Any]
    cache: Union[RequestsCache, None]
    close_client: bool
    close_cache: bool

    def __init__(
        self,
        existing_client: Union[httpx.Client, httpx.AsyncClient, None],
        kwargs: Dict[str, Any],
        cache: Union[RequestsCache, None],
    ):
        self.client = existing_client
        self.kwargs = kwargs
        self.cache = cache
        self.close_client = False
        self.close_cache = False

    def __enter__(self) -> httpx.Client:
        self.connect_to_cache()
        if self.client is None:
            self.client = httpx.Client(**self.kwargs)
            self.close_client = True
        return self.client

    def __exit__(
        self,
        exc_type: Type[BaseException] = None,
        exc_val: BaseException = None,
        exc_tb: TracebackType = None,
    ) -> None:
        if self.close_cache:
            self.cache.close()
        if self.close_client:
            self.client.close()

    async def __aenter__(self) -> httpx.AsyncClient:
        self.connect_to_cache()
        if self.client is None:
            self.client = httpx.AsyncClient(**self.kwargs)
            self.close_client = True
        return self.client

    async def __aexit__(
        self,
        exc_type: Type[BaseException] = None,
        exc_val: BaseException = None,
        exc_tb: TracebackType = None,
    ) -> None:
        if self.close_cache:
            self.cache.close()
        if self.close_client:
            await self.client.aclose()

    def connect_to_cache(self) -> None:
        if self.cache and not self.cache.is_open():
            self.cache.open()
            self.close_cache = True
