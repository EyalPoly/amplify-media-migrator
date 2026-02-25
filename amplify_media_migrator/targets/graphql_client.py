import logging
from dataclasses import dataclass
from typing import Any, Dict, List, NoReturn, Optional

import requests

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


@dataclass
class Media:
    id: str
    url: str
    observation_id: str
    type: MediaType
    is_available_for_public_use: bool


_QUERY_OBSERVATION_BY_SEQUENTIAL_ID = """
query GetObservationBySequentialId($sequentialId: Int!) {
  listObservations(filter: { sequentialId: { eq: $sequentialId } }) {
    items {
      id
      sequentialId
    }
  }
}
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
query GetMediaByUrl($url: AWSUrl!) {
  listMedia(filter: { url: { eq: $url } }) {
    items {
      id
      url
      observationId
      type
      isAvailableForPublicUse
    }
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
            response = requests.post(
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
        self, sequential_id: int
    ) -> Optional[Observation]:
        data = self._execute(
            _QUERY_OBSERVATION_BY_SEQUENTIAL_ID,
            variables={"sequentialId": sequential_id},
            operation="GetObservationBySequentialId",
        )

        items = data.get("listObservations", {}).get("items", [])
        if not items:
            return None

        item = items[0]
        return Observation(
            id=item["id"],
            sequential_id=item["sequentialId"],
        )

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
        data = self._execute(
            _QUERY_MEDIA_BY_URL,
            variables={"url": url},
            operation="GetMediaByUrl",
        )

        items = data.get("listMedia", {}).get("items", [])
        if not items:
            return None

        item = items[0]
        return Media(
            id=item["id"],
            url=item["url"],
            observation_id=item["observationId"],
            type=MediaType(item["type"]),
            is_available_for_public_use=item["isAvailableForPublicUse"],
        )
