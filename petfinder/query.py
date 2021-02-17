from typing import (
    Awaitable,
    Callable,
    Optional,
    Generic,
    TypeVar,
    Dict,
    Any,
    ClassVar,
    Type,
    Iterable,
)

import httpx
from pydantic.main import BaseModel

R = TypeVar("R")
T = TypeVar("T")


class QueryParams(BaseModel):
    class Config(BaseModel.Config):
        use_enum_values = True

        @staticmethod
        def convert_value_to_query_string(v: Any) -> str:
            """
            Convert a python value for a query parameter into the string value the API is expecting.
            There are varying standards for stuff like how an array of values should be represented,
            so you should override this method to format in accordance with your API.
            """
            return ",".join(v) if isinstance(v, list) else v

    def dict(self, *args, **kwargs):
        kwargs.setdefault("exclude_none", True)
        kwargs.setdefault("by_alias", True)
        return {
            k: self.Config.convert_value_to_query_string(v)
            for k, v in super().dict(*args, **kwargs).items()
        }


class Query(Generic[R]):
    params_class: ClassVar[Optional[Type[QueryParams]]] = None
    path: str
    params: Dict[str, Any]
    executor: Optional[Callable[["Query[R]"], R]]
    async_executor: Optional[Callable[["Query[R]"], Awaitable[R]]]
    async_batch_executor: Optional[
        Callable[["Iterable[Query[R]]"], Awaitable[Iterable[R]]]
    ]
    _kwargs: dict

    def __init__(
        self,
        *,
        path: str,
        executor: Optional[Callable[["Query[R]"], R]] = None,
        async_executor: Optional[Callable[["Query[R]"], Awaitable[R]]] = None,
        async_batch_executor: Optional[
            Callable[["Iterable[Query[R]]"], Awaitable[Iterable[R]]]
        ] = None,
        params: Dict[str, Any] = None,
        **kwargs,
    ) -> None:
        self.path = path
        self.params = params or {}
        self.executor = executor
        self.async_executor = async_executor
        self.async_batch_executor = async_batch_executor
        self._kwargs = kwargs

    def _chain(self: T, **params) -> T:
        """
        Returns a new instance of this class with the additional query parameters
        """
        return self.__class__(
            path=self.path,
            params={**self.params, **params},
            executor=self.executor,
            async_executor=self.async_executor,
            async_batch_executor=self.async_batch_executor,
            **self._kwargs,
        )

    def new_query(self, path: str, **params) -> "Query[Any]":
        """
        Returns a new query (with a different path and params)
        """
        return Query(
            path=path,
            params=params,
            executor=self.executor,
            async_executor=self.async_executor,
            async_batch_executor=self.async_batch_executor,
        )

    def execute(self) -> R:
        """
        Executes this query asynchronously
        """
        return self.executor(self)

    async def async_execute(self) -> R:
        """
        Returns a coroutine for executing this query asynchronously
        """
        return await self.async_executor(self)

    def __str__(self) -> str:
        if self.params:
            params = (
                self.params_class.construct(**self.params).dict()
                if self.params_class
                else self.params
            )
            return f"{self.path}?{httpx.QueryParams(params)}"
        return self.path
