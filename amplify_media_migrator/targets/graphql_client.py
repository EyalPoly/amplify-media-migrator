import logging
import re
import threading
from dataclasses import dataclass
from typing import Any, Dict, List, NoReturn, Optional

import requests
import requests.adapters

from ..utils.exceptions import (
    AuthenticationError,
    GraphQLError,
    RateLimitError,
)
from ..utils.media import MediaType

logger = logging.getLogger(__name__)


@dataclass
class Observation:
    id: str
    sequential_id: int
    discriminator_value: Optional[str] = None


@dataclass
class Media:
    id: str
    url: str
    observation_id: str
    type: MediaType
    is_available_for_public_use: bool


_FIELD_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _build_observation_query(discriminator_field: Optional[str]) -> str:
    extra = ""
    if discriminator_field:
        if not _FIELD_RE.match(discriminator_field):
            raise ValueError(f"Invalid discriminator_field: {discriminator_field!r}")
        extra = f"\n      {discriminator_field}"
    return f"""
query GetObservationBySequentialId($sequentialId: Int!, $nextToken: String) {{
  listObservations(filter: {{ sequentialId: {{ eq: $sequentialId }} }}, limit: 10000, nextToken: $nextToken) {{
    items {{
      id
      sequentialId{extra}
    }}
    nextToken
  }}
}}
"""


_MUTATION_CREATE_MEDIA = """
mutation CreateMedia($input: CreateMediaInput!) {
  createMedia(input: $input) {
    id
    url
    observationId
    type
    isAvailableForPublicUse
  }
}
"""

_QUERY_MEDIA_BY_URL = """
query GetMediaByUrl($url: String!, $nextToken: String) {
  listMedia(filter: { url: { eq: $url } }, limit: 10000, nextToken: $nextToken) {
    items {
      id
      url
      observationId
      type
      isAvailableForPublicUse
    }
    nextToken
  }
}
"""


class GraphQLClient:
    def __init__(
        self,
        api_endpoint: str,
        region: str = "us-east-1",
    ) -> None:
        self._api_endpoint = api_endpoint
        self._region = region
        self._id_token: Optional[str] = None
        self._local = threading.local()
        self._sessions_lock = threading.Lock()
        self._all_sessions: List[requests.Session] = []

    def _get_session(self) -> requests.Session:
        if not hasattr(self._local, "session"):
            s = requests.Session()
            # Each thread keeps one persistent HTTPS connection to AppSync,
            # eliminating per-request TLS handshake overhead.
            adapter = requests.adapters.HTTPAdapter(pool_connections=1, pool_maxsize=1)
            s.mount("https://", adapter)
            self._local.session = s
            with self._sessions_lock:
                self._all_sessions.append(s)
        session: requests.Session = self._local.session
        return session

    def close(self) -> None:
        # Sessions live in per-thread storage, so the calling thread cannot
        # reach other threads' slots. Close every registered session instead,
        # and reset the thread-locals so any later call creates a fresh one.
        with self._sessions_lock:
            sessions = self._all_sessions
            self._all_sessions = []
            self._local = threading.local()
        for session in sessions:
            session.close()

    def connect(self, id_token: str) -> None:
        self._id_token = id_token
        logger.info("GraphQL client connected with auth token")

    def _ensure_connected(self) -> str:
        if self._id_token is None:
            raise GraphQLError(
                "Not connected to GraphQL API. Call connect() first.",
                operation=None,
            )
        return self._id_token

    @staticmethod
    def _handle_response_error(
        status_code: int,
        response_text: str,
        operation: Optional[str] = None,
    ) -> NoReturn:
        if status_code in (401, 403):
            raise AuthenticationError(
                f"GraphQL authentication error ({status_code}): {response_text}",
                provider="cognito",
            )

        if status_code == 429:
            raise RateLimitError(
                f"GraphQL API rate limit exceeded: {response_text}",
            )

        raise GraphQLError(
            f"GraphQL HTTP error ({status_code}): {response_text}",
            operation=operation,
        )

    def _execute(
        self,
        query: str,
        variables: Optional[Dict[str, Any]] = None,
        operation: Optional[str] = None,
    ) -> Dict[str, Any]:
        id_token = self._ensure_connected()
        headers = {
            "Authorization": id_token,
            "Content-Type": "application/json",
        }
        payload: Dict[str, Any] = {
            "query": query,
            "variables": variables or {},
        }

        try:
            response = self._get_session().post(
                self._api_endpoint,
                headers=headers,
                json=payload,
                timeout=30,
            )
        except requests.exceptions.RequestException as e:
            raise GraphQLError(
                f"GraphQL request failed: {e}",
                operation=operation,
            ) from e

        if response.status_code != 200:
            self._handle_response_error(response.status_code, response.text, operation)

        result: Dict[str, Any] = response.json()

        if "errors" in result:
            errors = result["errors"]
            error_messages = [e.get("message", str(e)) for e in errors]
            raise GraphQLError(
                f"GraphQL errors in {operation}: {error_messages}",
                operation=operation,
                errors=errors,
            )

        data: Dict[str, Any] = result.get("data", {})
        return data

    def get_observation_by_sequential_id(
        self, sequential_id: int, discriminator_field: Optional[str] = None
    ) -> Optional[Observation]:
        query = _build_observation_query(discriminator_field)
        next_token: Optional[str] = None

        while True:
            variables: Dict[str, Any] = {"sequentialId": sequential_id}
            if next_token:
                variables["nextToken"] = next_token

            data = self._execute(
                query,
                variables=variables,
                operation="GetObservationBySequentialId",
            )

            list_data = data.get("listObservations", {})
            items = list_data.get("items", [])
            if items:
                item = items[0]
                return Observation(
                    id=item["id"],
                    sequential_id=item["sequentialId"],
                    discriminator_value=(
                        item.get(discriminator_field) if discriminator_field else None
                    ),
                )

            next_token = list_data.get("nextToken")
            if not next_token:
                return None

    def get_all_observations_by_sequential_id(
        self, sequential_id: int, discriminator_field: Optional[str] = None
    ) -> List[Observation]:
        query = _build_observation_query(discriminator_field)
        results: List[Observation] = []
        next_token: Optional[str] = None

        while True:
            variables: Dict[str, Any] = {"sequentialId": sequential_id}
            if next_token:
                variables["nextToken"] = next_token

            data = self._execute(
                query,
                variables=variables,
                operation="GetObservationBySequentialId",
            )

            list_data = data.get("listObservations", {})
            for item in list_data.get("items", []):
                results.append(
                    Observation(
                        id=item["id"],
                        sequential_id=item["sequentialId"],
                        discriminator_value=(
                            item.get(discriminator_field)
                            if discriminator_field
                            else None
                        ),
                    )
                )

            next_token = list_data.get("nextToken")
            if not next_token:
                return results

    def get_observations_by_sequential_ids(
        self, sequential_ids: List[int]
    ) -> Dict[int, Observation]:
        results: Dict[int, Observation] = {}
        for seq_id in sequential_ids:
            observation = self.get_observation_by_sequential_id(seq_id)
            if observation is not None:
                results[seq_id] = observation
        return results

    def create_media(
        self,
        url: str,
        observation_id: str,
        media_type: MediaType,
        is_public: bool = False,
    ) -> Media:
        data = self._execute(
            _MUTATION_CREATE_MEDIA,
            variables={
                "input": {
                    "url": url,
                    "observationId": observation_id,
                    "type": media_type.value,
                    "isAvailableForPublicUse": is_public,
                }
            },
            operation="CreateMedia",
        )

        item = data.get("createMedia", {})
        return Media(
            id=item["id"],
            url=item["url"],
            observation_id=item["observationId"],
            type=MediaType(item["type"]),
            is_available_for_public_use=item["isAvailableForPublicUse"],
        )

    def get_media_by_url(self, url: str) -> Optional[Media]:
        next_token: Optional[str] = None

        while True:
            variables: Dict[str, Any] = {"url": url}
            if next_token:
                variables["nextToken"] = next_token

            data = self._execute(
                _QUERY_MEDIA_BY_URL,
                variables=variables,
                operation="GetMediaByUrl",
            )

            list_data = data.get("listMedia", {})
            items = list_data.get("items", [])
            if items:
                item = items[0]
                return Media(
                    id=item["id"],
                    url=item["url"],
                    observation_id=item["observationId"],
                    type=MediaType(item["type"]),
                    is_available_for_public_use=item["isAvailableForPublicUse"],
                )

            next_token = list_data.get("nextToken")
            if not next_token:
                return None
