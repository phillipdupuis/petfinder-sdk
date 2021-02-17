from typing import Generator

from httpx import Auth, Request, Response


class TokenAuth(Auth):
    requires_response_body = True

    def __init__(self, secret: str, api_key: str, token_url: str) -> None:
        self.secret = secret
        self.api_key = api_key
        self.token_url = token_url
        self.token = None

    def auth_flow(self, request: Request) -> Generator[Request, Response, None]:
        """
        Authentication works by passing requests into this 'flow' function, which is a generator.
        It yields requests, and the calling function sends in the responses.

        The authentication 'flow' ends when the generator is exhausted, and the final response is
        used as the real one.
        """
        request.headers["Authorization"] = f"Bearer {self.token}"
        response = yield request

        if response.status_code == 401:
            token_response = yield self.build_token_request()
            self.update_token(token_response)
            request.headers["Authorization"] = f"Bearer {self.token}"
            yield request

    def build_token_request(self) -> Request:
        return Request(
            method="POST",
            url=self.token_url,
            data={
                "grant_type": "client_credentials",
                "client_id": self.api_key,
                "client_secret": self.secret,
            },
        )

    def update_token(self, response: Response) -> None:
        response.raise_for_status()
        self.token = response.json()["access_token"]
