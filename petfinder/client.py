from typing import Generic, Optional, Callable

import httpx
from httpx._types import ProxiesTypes

from petfinder.animals import AnimalsQuery
from petfinder.cache import ResponseCache
from petfinder.enums import Category
from petfinder.query import Query
from petfinder.static_data import StaticData, CategoryData
from petfinder.types import HttpClient


class PetfinderClient(Generic[HttpClient]):
    animals: AnimalsQuery
    async_: bool
    response_cache: Optional[ResponseCache]
    static_data: StaticData
    http_client: Callable[[], HttpClient]

    _api_secret: str
    _api_key: str
    _base_url: str
    _proxies: ProxiesTypes
    _authorization_headers: dict

    def __init__(
        self,
        *,
        api_secret: str,
        api_key: str,
        async_: bool = False,
        response_cache: Optional[ResponseCache] = None,
        proxies: ProxiesTypes = None,
        base_url: str = "https://api.petfinder.com/v2",
    ) -> None:
        self.async_ = async_
        self.http_client = self._async_client if async_ else self._client
        self.response_cache = response_cache
        self._api_secret = api_secret
        self._api_key = api_key
        self._proxies = proxies
        self._base_url = base_url
        self._refresh_access_token()
        self._initialize_static_data()
        self.animals = AnimalsQuery(
            async_=async_,
            get_client=self.http_client,
            refresh_access_token=self._refresh_access_token,
            response_cache=self.response_cache,
            static_data=self.static_data,
        )

    def _client(self) -> httpx.Client:
        return httpx.Client(
            base_url=self._base_url,
            headers=self._authorization_headers,
            proxies=self._proxies,
        )

    def _async_client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url=self._base_url,
            headers=self._authorization_headers,
            proxies=self._proxies,
        )

    def _refresh_access_token(self) -> None:
        """
        Retrieves a new access token and updates the auth headers.
        """
        response = httpx.post(
            f"{self._base_url}/oauth2/token",
            proxies=self._proxies,
            data={
                "grant_type": "client_credentials",
                "client_id": self._api_key,
                "client_secret": self._api_secret,
            },
        )
        response.raise_for_status()
        token = response.json()["access_token"]
        self._authorization_headers = {"Authorization": f"Bearer {token}"}

    def _initialize_static_data(self) -> None:
        """
        Sets up static data (types, breeds, etc) so other parts of the client can rely on that
        data being available. This helps quite a bit with performance and reducing API calls.
        """
        q = Query(
            path="",
            async_=False,
            get_client=self._client,
            refresh_access_token=self._refresh_access_token,
            response_cache=self.response_cache,
        )
        data = {}
        for c in list(Category):
            types_response = q._new_query(path=f"types/{c}").execute()
            breeds_response = q._new_query(path=f"types/{c}/breeds").execute()
            data[c] = CategoryData(
                breeds=set(x["name"].lower() for x in breeds_response["breeds"]),
                coats=set(x.lower() for x in types_response["type"]["coats"]),
                colors=set(x.lower() for x in types_response["type"]["colors"]),
                genders=set(x.lower() for x in types_response["type"]["genders"]),
            )
        self.static_data = StaticData(data)
