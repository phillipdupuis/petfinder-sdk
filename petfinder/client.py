import abc
from typing import List

import httpx
from httpx._types import ProxiesTypes

from petfinder.models import AnimalQuery


class BaseClient(abc.ABC):
    _base_url: str
    _api_secret: str
    _api_key: str
    _authorization_headers: dict
    _proxies: ProxiesTypes

    def __init__(
        self,
        *,
        api_secret: str,
        api_key: str,
        proxies: ProxiesTypes = None,
        base_url: str = "https://api.petfinder.com/v2",
    ) -> None:
        self._api_secret = api_secret
        self._api_key = api_key
        self._proxies = proxies
        self._base_url = base_url
        self._refresh_access_token()

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
        c = httpx.Client()
        c.close()
        self._authorization_headers = {"Authorization": f"Bearer {token}"}


class Client(BaseClient):
    def _client(self) -> httpx.Client:
        """
        Sets up the httpx client
        """
        return httpx.Client(
            base_url=self._base_url, headers=self._authorization_headers
        )

    def _get(self, path: str, **params) -> dict:
        """
        Performs a GET request to the petfinder API.

        If the request fails due to an authentication error (401) we assume
        that the token has simply expired, so we refresh it and then call
        this function again.
        """
        try:
            with self._client() as client:
                response = client.get(path, params=params)
                response.raise_for_status()
                return response.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                self._refresh_access_token()
                return self._get(path, **params)
            else:
                raise e

    def get_animals(
        self, query: AnimalQuery = None, require_photos: bool = False
    ) -> List[dict]:
        """
        Retrieves a page of animals satisfying the given constraints
        """
        params = query.dict() if query else {}
        data = self._get(path="animals", **params)
        animals = data["animals"]
        return [a for a in animals if a.get("photos")] if require_photos else animals


class AsyncClient(BaseClient):
    def _client(self) -> httpx.AsyncClient:
        """
        Sets up the httpx async client
        """
        return httpx.AsyncClient(
            base_url=self._base_url, headers=self._authorization_headers
        )

    async def _get(self, path: str, **params) -> dict:
        """
        Performs a GET request to the petfinder API.

        If the request fails due to an authentication error (401) we assume
        that the token has simply expired, so we refresh it and then call
        this function again.
        """
        try:
            async with self._client() as client:
                response = await client.get(path, params=params)
                response.raise_for_status()
                return response.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                self._refresh_access_token()
                return await self._get(path, **params)
            else:
                raise e

    async def get_animals(
        self, query: AnimalQuery = None, require_photos: bool = False
    ) -> List[dict]:
        """
        Retrieves a page of animals satisfying the given constraints
        """
        params = query.dict() if query else {}
        data = await self._get(path="animals", **params)
        animals = data["animals"]
        return [a for a in animals if a.get("photos")] if require_photos else animals
