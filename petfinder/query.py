from typing import (
    Callable,
    Optional,
    Generic,
    TypeVar,
    Union,
    Dict,
    Any,
    ClassVar,
    Type,
)

import httpx
from pydantic.main import BaseModel

from petfinder.cache import ResponseCache
from petfinder.static_data import StaticData
from petfinder.types import HttpClient, MaybeAwaitable

ResponseSchema = TypeVar("ResponseSchema")


def _as_query_string(v: Any) -> str:
    return ",".join(v) if isinstance(v, list) else v


class QueryParams(BaseModel):
    class Config(BaseModel.Config):
        use_enum_values = True

    def dict(self, *args, **kwargs):
        kwargs.setdefault("exclude_none", True)
        kwargs.setdefault("by_alias", True)
        return {
            k: _as_query_string(v) for k, v in super().dict(*args, **kwargs).items()
        }


class Query(Generic[ResponseSchema]):
    params_class: ClassVar[Optional[Type[QueryParams]]] = None

    path: str
    params: Dict[str, Any]
    execute: Callable[[], MaybeAwaitable[ResponseSchema]]
    _async: bool
    _get_client: Callable[[], HttpClient]
    _refresh_access_token: Callable[[], None]
    _response_cache: Optional[ResponseCache]
    _static_data: StaticData
    _kwargs: dict

    def __init__(
        self,
        *,
        path: str,
        async_: bool,
        get_client: Callable[[], HttpClient],
        refresh_access_token: Callable[[], None],
        response_cache: Optional[ResponseCache] = None,
        params: Dict[str, Any] = None,
        static_data: StaticData = None,
        **kwargs,
    ) -> None:
        self.path = path
        self.params = params or {}
        self.execute = self._async_execute if async_ else self._execute
        self._async = async_
        self._get_client = get_client
        self._refresh_access_token = refresh_access_token
        self._response_cache = response_cache
        self._static_data = static_data
        self._kwargs = kwargs

    def _chain(self, **new_query_params) -> "Query":
        """
        Returns a new instance of this class with the additional query parameters
        """
        return self.__class__(
            path=self.path,
            params={**self.params, **new_query_params},
            async_=self._async,
            get_client=self._get_client,
            refresh_access_token=self._refresh_access_token,
            response_cache=self._response_cache,
            static_data=self._static_data,
            **self._kwargs,
        )

    def _new_query(self, path: str, **params) -> "Query":
        """
        Returns a new query (with a different path and params)
        """
        return Query(
            path=path,
            params=params,
            async_=self._async,
            get_client=self._get_client,
            refresh_access_token=self._refresh_access_token,
            response_cache=self._response_cache,
            static_data=self._static_data,
        )

    def _build_request(self, client: HttpClient) -> httpx.Request:
        """
        Build and return an httpx Request for this query.

        If a pydantic model for parsing/validating query parameters has been defined,
        we will now use it to parse the raw query params into a finalized form.
        """
        params = (
            self.params
            if self.params_class is None
            else self.params_class(
                __static_data__=self._static_data, **self.params
            ).dict()
        )
        return client.build_request("GET", self.path, params=params)

    def _get_cached_response(
        self, request: httpx.Request
    ) -> Union[ResponseSchema, None]:
        """
        If a response cache exists and a cached response exists, we return that.
        This helps cut down on unnecessary API calls.
        """
        if self._response_cache is not None and self._response_cache.has_response(
            request
        ):
            return self._response_cache.get_response(request)
        return None

    def _process_response(
        self, request: httpx.Request, response: httpx.Response
    ) -> ResponseSchema:
        """
        Check for errors, cache the response data (if appropriate), and then return it.
        """
        response.raise_for_status()
        if self._response_cache is not None:
            self._response_cache.save_response(request, response)
        return response.json()

    def _execute(self) -> ResponseSchema:
        """
        Standard, synchronous execution of a query.
        """
        try:
            with self._get_client() as client:
                request = self._build_request(client)

                cached_response = self._get_cached_response(request)
                if cached_response is not None:
                    return cached_response

                response = client.send(request)
                return self._process_response(request, response)

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                self._refresh_access_token()
                return self._execute()
            else:
                raise e

    # def _execute2(self, client: httpx.Client = None) -> ResponseSchema:
    #     """
    #     Standard, synchronous execution of a query.
    #     """
    #     handling_connections = True if client is None else False
    #
    #     try:
    #         if handling_connections:
    #             client = self._get_client()
    #
    #         request = self._build_request(client)
    #
    #         cached_response = self._get_cached_response(request)
    #         if cached_response is not None:
    #             return cached_response
    #
    #         response = client.send(request)
    #         return self._process_response(request, response)
    #
    #     except httpx.HTTPStatusError as e:
    #         if handling_connections and e.response.status_code == 401:
    #             self._refresh_access_token()
    #             return self._execute()
    #         else:
    #             raise e
    #
    #     finally:
    #         if handling_connections:
    #             client.close()

    async def _async_execute(self) -> ResponseSchema:
        """
        Asynchronous execution of a query.
        """
        try:
            async with self._get_client() as client:
                request = self._build_request(client)

                cached_response = self._get_cached_response(request)
                if cached_response is not None:
                    return cached_response

                response = await client.send(request)
                return self._process_response(request, response)

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                self._refresh_access_token()
                return await self._async_execute()
            else:
                raise e

    def __str__(self) -> str:
        if self.params:
            params = (
                self.params_class.construct(**self.params).dict()
                if self.params_class
                else self.params
            )
            return f"{self.path}?{httpx.QueryParams(params)}"
        return self.path
