import threading
from typing import Any, List
from unittest.mock import MagicMock, patch

import pytest
import requests

from amplify_media_migrator.targets.graphql_client import (
    GraphQLClient,
    Media,
    Observation,
)
from amplify_media_migrator.utils.exceptions import (
    AuthenticationError,
    GraphQLError,
    RateLimitError,
)
from amplify_media_migrator.utils.media import MediaType

API_ENDPOINT = "https://test.appsync-api.us-east-1.amazonaws.com/graphql"
ID_TOKEN = "test-id-token-abc123"


@pytest.fixture
def client() -> GraphQLClient:
    return GraphQLClient(API_ENDPOINT)


@pytest.fixture
def connected_client(client: GraphQLClient) -> GraphQLClient:
    client.connect(ID_TOKEN)
    return client


def _make_response(
    status_code: int = 200,
    json_data: dict = None,
    text: str = "",
) -> MagicMock:
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status_code
    resp.json.return_value = json_data or {}
    resp.text = text
    return resp


class TestInit:
    def test_stores_endpoint(self) -> None:
        client = GraphQLClient(API_ENDPOINT)
        assert client._api_endpoint == API_ENDPOINT

    def test_default_region(self) -> None:
        client = GraphQLClient(API_ENDPOINT)
        assert client._region == "us-east-1"

    def test_custom_region(self) -> None:
        client = GraphQLClient(API_ENDPOINT, region="eu-west-1")
        assert client._region == "eu-west-1"

    def test_not_connected_initially(self, client: GraphQLClient) -> None:
        assert client._id_token is None


class TestConnect:
    def test_stores_token(self, client: GraphQLClient) -> None:
        client.connect(ID_TOKEN)
        assert client._id_token == ID_TOKEN


class TestEnsureConnected:
    def test_raises_when_not_connected(self, client: GraphQLClient) -> None:
        with pytest.raises(GraphQLError, match="Not connected"):
            client._ensure_connected()

    def test_returns_token_when_connected(
        self, connected_client: GraphQLClient
    ) -> None:
        assert connected_client._ensure_connected() == ID_TOKEN


class TestHandleResponseError:
    def test_401_raises_auth_error(self) -> None:
        with pytest.raises(AuthenticationError, match="401") as exc_info:
            GraphQLClient._handle_response_error(401, "Unauthorized")
        assert exc_info.value.provider == "cognito"

    def test_403_raises_auth_error(self) -> None:
        with pytest.raises(AuthenticationError, match="403"):
            GraphQLClient._handle_response_error(403, "Forbidden")

    def test_500_raises_graphql_error(self) -> None:
        with pytest.raises(GraphQLError, match="500") as exc_info:
            GraphQLClient._handle_response_error(
                500, "Internal Server Error", operation="TestOp"
            )
        assert exc_info.value.operation == "TestOp"

    def test_429_raises_rate_limit_error(self) -> None:
        with pytest.raises(RateLimitError, match="rate limit"):
            GraphQLClient._handle_response_error(429, "Too Many Requests")


class TestExecute:
    @patch("requests.Session.post")
    def test_sends_correct_request(
        self, mock_post: MagicMock, connected_client: GraphQLClient
    ) -> None:
        mock_post.return_value = _make_response(json_data={"data": {"result": "ok"}})

        connected_client._execute("query { test }", variables={"x": 1})

        mock_post.assert_called_once_with(
            API_ENDPOINT,
            headers={
                "Authorization": ID_TOKEN,
                "Content-Type": "application/json",
            },
            json={"query": "query { test }", "variables": {"x": 1}},
            timeout=30,
        )

    @patch("requests.Session.post")
    def test_returns_data(
        self, mock_post: MagicMock, connected_client: GraphQLClient
    ) -> None:
        mock_post.return_value = _make_response(
            json_data={"data": {"items": [1, 2, 3]}}
        )

        result = connected_client._execute("query { items }")

        assert result == {"items": [1, 2, 3]}

    @patch("requests.Session.post")
    def test_graphql_errors_raise(
        self, mock_post: MagicMock, connected_client: GraphQLClient
    ) -> None:
        mock_post.return_value = _make_response(
            json_data={
                "data": None,
                "errors": [{"message": "Validation error"}],
            }
        )

        with pytest.raises(GraphQLError) as exc_info:
            connected_client._execute("query { bad }", operation="TestOp")

        assert exc_info.value.operation == "TestOp"
        assert len(exc_info.value.errors) == 1

    @patch("requests.Session.post")
    def test_http_401_raises_auth_error(
        self, mock_post: MagicMock, connected_client: GraphQLClient
    ) -> None:
        mock_post.return_value = _make_response(status_code=401, text="Unauthorized")

        with pytest.raises(AuthenticationError):
            connected_client._execute("query { test }")

    @patch("requests.Session.post")
    def test_http_500_raises_graphql_error(
        self, mock_post: MagicMock, connected_client: GraphQLClient
    ) -> None:
        mock_post.return_value = _make_response(
            status_code=500, text="Internal Server Error"
        )

        with pytest.raises(GraphQLError, match="500"):
            connected_client._execute("query { test }")

    @patch("requests.Session.post")
    def test_network_error_raises_graphql_error(
        self, mock_post: MagicMock, connected_client: GraphQLClient
    ) -> None:
        mock_post.side_effect = requests.exceptions.ConnectionError(
            "Connection refused"
        )

        with pytest.raises(GraphQLError, match="request failed"):
            connected_client._execute("query { test }", operation="TestOp")

    @patch("requests.Session.post")
    def test_timeout_raises_graphql_error(
        self, mock_post: MagicMock, connected_client: GraphQLClient
    ) -> None:
        mock_post.side_effect = requests.exceptions.Timeout("Request timed out")

        with pytest.raises(GraphQLError, match="request failed"):
            connected_client._execute("query { test }")

    def test_not_connected_raises(self, client: GraphQLClient) -> None:
        with pytest.raises(GraphQLError, match="Not connected"):
            client._execute("query { test }")


class TestGetObservationBySequentialId:
    @patch("requests.Session.post")
    def test_returns_observation(
        self, mock_post: MagicMock, connected_client: GraphQLClient
    ) -> None:
        mock_post.return_value = _make_response(
            json_data={
                "data": {
                    "listObservations": {
                        "items": [{"id": "obs-123", "sequentialId": 6602}]
                    }
                }
            }
        )

        result = connected_client.get_observation_by_sequential_id(6602)

        assert result == Observation(id="obs-123", sequential_id=6602)

    @patch("requests.Session.post")
    def test_returns_none_when_not_found(
        self, mock_post: MagicMock, connected_client: GraphQLClient
    ) -> None:
        mock_post.return_value = _make_response(
            json_data={"data": {"listObservations": {"items": []}}}
        )

        result = connected_client.get_observation_by_sequential_id(99999)

        assert result is None

    @patch("requests.Session.post")
    def test_returns_first_when_multiple(
        self, mock_post: MagicMock, connected_client: GraphQLClient
    ) -> None:
        mock_post.return_value = _make_response(
            json_data={
                "data": {
                    "listObservations": {
                        "items": [
                            {"id": "obs-1", "sequentialId": 100},
                            {"id": "obs-2", "sequentialId": 100},
                        ]
                    }
                }
            }
        )

        result = connected_client.get_observation_by_sequential_id(100)

        assert result is not None
        assert result.id == "obs-1"

    @patch("requests.Session.post")
    def test_paginates_to_find_observation(
        self, mock_post: MagicMock, connected_client: GraphQLClient
    ) -> None:
        mock_post.side_effect = [
            _make_response(
                json_data={
                    "data": {
                        "listObservations": {
                            "items": [],
                            "nextToken": "token-page-2",
                        }
                    }
                }
            ),
            _make_response(
                json_data={
                    "data": {
                        "listObservations": {
                            "items": [{"id": "obs-123", "sequentialId": 6001}],
                            "nextToken": None,
                        }
                    }
                }
            ),
        ]

        result = connected_client.get_observation_by_sequential_id(6001)

        assert result == Observation(id="obs-123", sequential_id=6001)
        assert mock_post.call_count == 2
        second_call_variables = mock_post.call_args_list[1][1]["json"]["variables"]
        assert second_call_variables["nextToken"] == "token-page-2"

    @patch("requests.Session.post")
    def test_returns_none_after_all_pages_exhausted(
        self, mock_post: MagicMock, connected_client: GraphQLClient
    ) -> None:
        mock_post.side_effect = [
            _make_response(
                json_data={
                    "data": {
                        "listObservations": {
                            "items": [],
                            "nextToken": "token-2",
                        }
                    }
                }
            ),
            _make_response(
                json_data={
                    "data": {
                        "listObservations": {
                            "items": [],
                            "nextToken": None,
                        }
                    }
                }
            ),
        ]

        result = connected_client.get_observation_by_sequential_id(99999)

        assert result is None
        assert mock_post.call_count == 2


class TestGetObservationsBySequentialIds:
    @patch("requests.Session.post")
    def test_returns_found_observations(
        self, mock_post: MagicMock, connected_client: GraphQLClient
    ) -> None:
        mock_post.return_value = _make_response(
            json_data={
                "data": {
                    "listObservations": {
                        "items": [{"id": "obs-1", "sequentialId": 6000}]
                    }
                }
            }
        )

        result = connected_client.get_observations_by_sequential_ids([6000])

        assert 6000 in result
        assert result[6000].id == "obs-1"

    @patch("requests.Session.post")
    def test_skips_not_found(
        self, mock_post: MagicMock, connected_client: GraphQLClient
    ) -> None:
        mock_post.side_effect = [
            _make_response(
                json_data={
                    "data": {
                        "listObservations": {
                            "items": [{"id": "obs-1", "sequentialId": 6000}]
                        }
                    }
                }
            ),
            _make_response(json_data={"data": {"listObservations": {"items": []}}}),
        ]

        result = connected_client.get_observations_by_sequential_ids([6000, 6001])

        assert len(result) == 1
        assert 6000 in result
        assert 6001 not in result

    @patch("requests.Session.post")
    def test_empty_list(
        self, mock_post: MagicMock, connected_client: GraphQLClient
    ) -> None:
        result = connected_client.get_observations_by_sequential_ids([])

        assert result == {}
        mock_post.assert_not_called()


class TestCreateMedia:
    @patch("requests.Session.post")
    def test_creates_image_media(
        self, mock_post: MagicMock, connected_client: GraphQLClient
    ) -> None:
        mock_post.return_value = _make_response(
            json_data={
                "data": {
                    "createMedia": {
                        "id": "media-1",
                        "url": "https://bucket.s3.amazonaws.com/media/obs-1/photo.jpg",
                        "observationId": "obs-1",
                        "type": "PHOTO",
                        "isAvailableForPublicUse": False,
                    }
                }
            }
        )

        result = connected_client.create_media(
            url="https://bucket.s3.amazonaws.com/media/obs-1/photo.jpg",
            observation_id="obs-1",
            media_type=MediaType.IMAGE,
        )

        assert result == Media(
            id="media-1",
            url="https://bucket.s3.amazonaws.com/media/obs-1/photo.jpg",
            observation_id="obs-1",
            type=MediaType.IMAGE,
            is_available_for_public_use=False,
        )

    @patch("requests.Session.post")
    def test_creates_video_media(
        self, mock_post: MagicMock, connected_client: GraphQLClient
    ) -> None:
        mock_post.return_value = _make_response(
            json_data={
                "data": {
                    "createMedia": {
                        "id": "media-2",
                        "url": "https://bucket.s3.amazonaws.com/media/obs-1/video.mp4",
                        "observationId": "obs-1",
                        "type": "VIDEO",
                        "isAvailableForPublicUse": True,
                    }
                }
            }
        )

        result = connected_client.create_media(
            url="https://bucket.s3.amazonaws.com/media/obs-1/video.mp4",
            observation_id="obs-1",
            media_type=MediaType.VIDEO,
            is_public=True,
        )

        assert result.type == MediaType.VIDEO
        assert result.is_available_for_public_use is True

    @patch("requests.Session.post")
    def test_sends_correct_variables(
        self, mock_post: MagicMock, connected_client: GraphQLClient
    ) -> None:
        mock_post.return_value = _make_response(
            json_data={
                "data": {
                    "createMedia": {
                        "id": "m-1",
                        "url": "https://example.com/photo.jpg",
                        "observationId": "obs-1",
                        "type": "PHOTO",
                        "isAvailableForPublicUse": False,
                    }
                }
            }
        )

        connected_client.create_media(
            url="https://example.com/photo.jpg",
            observation_id="obs-1",
            media_type=MediaType.IMAGE,
            is_public=False,
        )

        call_payload = mock_post.call_args[1]["json"]
        assert call_payload["variables"] == {
            "input": {
                "url": "https://example.com/photo.jpg",
                "observationId": "obs-1",
                "type": "PHOTO",
                "isAvailableForPublicUse": False,
            }
        }


class TestGetMediaByUrl:
    @patch("requests.Session.post")
    def test_returns_media(
        self, mock_post: MagicMock, connected_client: GraphQLClient
    ) -> None:
        mock_post.return_value = _make_response(
            json_data={
                "data": {
                    "listMedia": {
                        "items": [
                            {
                                "id": "media-1",
                                "url": "https://bucket.s3.amazonaws.com/media/obs-1/photo.jpg",
                                "observationId": "obs-1",
                                "type": "PHOTO",
                                "isAvailableForPublicUse": False,
                            }
                        ]
                    }
                }
            }
        )

        result = connected_client.get_media_by_url(
            "https://bucket.s3.amazonaws.com/media/obs-1/photo.jpg"
        )

        assert result is not None
        assert result.id == "media-1"
        assert result.type == MediaType.IMAGE

    @patch("requests.Session.post")
    def test_returns_none_when_not_found(
        self, mock_post: MagicMock, connected_client: GraphQLClient
    ) -> None:
        mock_post.return_value = _make_response(
            json_data={"data": {"listMedia": {"items": []}}}
        )

        result = connected_client.get_media_by_url(
            "https://bucket.s3.amazonaws.com/media/nonexistent.jpg"
        )

        assert result is None

    @patch("requests.Session.post")
    def test_paginates_to_find_media(
        self, mock_post: MagicMock, connected_client: GraphQLClient
    ) -> None:
        mock_post.side_effect = [
            _make_response(
                json_data={
                    "data": {
                        "listMedia": {
                            "items": [],
                            "nextToken": "media-token-2",
                        }
                    }
                }
            ),
            _make_response(
                json_data={
                    "data": {
                        "listMedia": {
                            "items": [
                                {
                                    "id": "media-1",
                                    "url": "https://bucket.s3.amazonaws.com/media/obs-1/photo.jpg",
                                    "observationId": "obs-1",
                                    "type": "PHOTO",
                                    "isAvailableForPublicUse": False,
                                }
                            ],
                            "nextToken": None,
                        }
                    }
                }
            ),
        ]

        result = connected_client.get_media_by_url(
            "https://bucket.s3.amazonaws.com/media/obs-1/photo.jpg"
        )

        assert result is not None
        assert result.id == "media-1"
        assert mock_post.call_count == 2


class TestGetMediaObservationIdsByUrl:
    @patch("requests.Session.post")
    def test_collects_observation_ids_across_pages(
        self, mock_post: MagicMock, connected_client: GraphQLClient
    ) -> None:
        url = "https://bucket.s3.amazonaws.com/media/obs-1000/1000-1001.jpg"
        mock_post.side_effect = [
            _make_response(
                json_data={
                    "data": {
                        "listMedia": {
                            "items": [
                                {
                                    "id": "media-a",
                                    "url": url,
                                    "observationId": "obs-1000",
                                    "type": "PHOTO",
                                    "isAvailableForPublicUse": False,
                                }
                            ],
                            "nextToken": "token-2",
                        }
                    }
                }
            ),
            _make_response(
                json_data={
                    "data": {
                        "listMedia": {
                            "items": [
                                {
                                    "id": "media-b",
                                    "url": url,
                                    "observationId": "obs-1001",
                                    "type": "PHOTO",
                                    "isAvailableForPublicUse": False,
                                }
                            ],
                            "nextToken": None,
                        }
                    }
                }
            ),
        ]

        result = connected_client.get_media_observation_ids_by_url(url)

        assert result == {"obs-1000", "obs-1001"}
        assert mock_post.call_count == 2

    @patch("requests.Session.post")
    def test_returns_empty_set_when_none_found(
        self, mock_post: MagicMock, connected_client: GraphQLClient
    ) -> None:
        mock_post.return_value = _make_response(
            json_data={"data": {"listMedia": {"items": []}}}
        )

        result = connected_client.get_media_observation_ids_by_url(
            "https://bucket.s3.amazonaws.com/media/nonexistent.jpg"
        )

        assert result == set()


def test_close_closes_sessions_from_all_threads(client: GraphQLClient) -> None:
    created: List[Any] = []
    add_lock = threading.Lock()

    def make_session() -> None:
        session = client._get_session()
        with add_lock:
            created.append(session)

    with patch("requests.Session", side_effect=lambda: MagicMock()):
        threads = [threading.Thread(target=make_session) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        client.close()

    assert len(created) == 3
    for session in created:
        session.close.assert_called_once_with()


def test_close_noop_when_no_sessions(client: GraphQLClient) -> None:
    client.close()

    with patch("requests.Session", side_effect=lambda: MagicMock()):
        session = client._get_session()
        client.close()
        client.close()

    session.close.assert_called_once_with()


def test_get_session_after_close_creates_new_session(client: GraphQLClient) -> None:
    with patch("requests.Session", side_effect=lambda: MagicMock()):
        first = client._get_session()
        client.close()
        second = client._get_session()

    assert first is not second
    first.close.assert_called_once_with()


class TestDiscriminatorLookup:
    def test_single_lookup_populates_discriminator_value(self):
        from amplify_media_migrator.targets.graphql_client import GraphQLClient

        client = GraphQLClient(api_endpoint="https://x/graphql")
        client.connect("token")
        client._execute = lambda *a, **k: {
            "listObservations": {
                "items": [{"id": "o1", "sequentialId": 5, "countryId": "c-red"}],
                "nextToken": None,
            }
        }
        obs = client.get_observation_by_sequential_id(
            5, discriminator_field="countryId"
        )
        assert obs is not None
        assert obs.discriminator_value == "c-red"

    def test_get_all_collects_every_candidate(self):
        from amplify_media_migrator.targets.graphql_client import GraphQLClient

        client = GraphQLClient(api_endpoint="https://x/graphql")
        client.connect("token")
        pages = [
            {
                "listObservations": {
                    "items": [{"id": "o1", "sequentialId": 5, "countryId": "c-med"}],
                    "nextToken": "t",
                }
            },
            {
                "listObservations": {
                    "items": [{"id": "o2", "sequentialId": 5, "countryId": "c-red"}],
                    "nextToken": None,
                }
            },
        ]
        client._execute = lambda *a, **k: pages.pop(0)
        result = client.get_all_observations_by_sequential_id(
            5, discriminator_field="countryId"
        )
        assert [o.id for o in result] == ["o1", "o2"]
        assert {o.discriminator_value for o in result} == {"c-med", "c-red"}

    def test_invalid_discriminator_field_rejected(self):
        from amplify_media_migrator.targets.graphql_client import GraphQLClient

        client = GraphQLClient(api_endpoint="https://x/graphql")
        client.connect("token")
        with pytest.raises(ValueError):
            client.get_all_observations_by_sequential_id(
                5, discriminator_field="bad field!"
            )


class TestConnectionResilience:
    @patch("requests.Session.post")
    def test_connection_error_resets_session(
        self, mock_post: MagicMock, connected_client: GraphQLClient
    ) -> None:
        mock_post.side_effect = requests.exceptions.ConnectionError(
            "('Connection aborted.', ConnectionResetError(54, 'Connection reset by peer'))"
        )
        first_session = connected_client._get_session()

        with pytest.raises(GraphQLError) as exc_info:
            connected_client._execute("query { x }", operation="Probe")

        assert exc_info.value.is_retryable is True
        assert not hasattr(connected_client._local, "session")
        assert first_session not in connected_client._all_sessions

    @patch("requests.Session.post")
    def test_connection_error_next_call_builds_fresh_session(
        self, mock_post: MagicMock, connected_client: GraphQLClient
    ) -> None:
        mock_post.side_effect = requests.exceptions.ConnectionError("reset")
        first_session = connected_client._get_session()

        with pytest.raises(GraphQLError):
            connected_client._execute("query { x }", operation="Probe")

        second_session = connected_client._get_session()
        assert second_session is not first_session

    @patch("requests.Session.post")
    def test_non_connection_request_error_does_not_reset(
        self, mock_post: MagicMock, connected_client: GraphQLClient
    ) -> None:
        mock_post.side_effect = requests.exceptions.Timeout("read timeout")
        first_session = connected_client._get_session()

        with pytest.raises(GraphQLError) as exc_info:
            connected_client._execute("query { x }", operation="Probe")

        assert exc_info.value.is_retryable is True
        assert connected_client._local.session is first_session
