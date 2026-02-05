from dataclasses import dataclass
from enum import Enum
from typing import Optional, List


class MediaType(Enum):
    IMAGE = "IMAGE"
    VIDEO = "VIDEO"


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

    def get_observation_by_sequential_id(self, sequential_id: int) -> Optional[Observation]:
        raise NotImplementedError

    def create_media(
        self,
        url: str,
        observation_id: str,
        media_type: MediaType,
        is_public: bool = False,
    ) -> Media:
        raise NotImplementedError

    def get_media_by_url(self, url: str) -> Optional[Media]:
        raise NotImplementedError

    def list_media_for_observation(self, observation_id: str) -> List[Media]:
        raise NotImplementedError

    def delete_media(self, media_id: str) -> None:
        raise NotImplementedError