import pytest

from amplify_media_migrator.targets.graphql_client import MediaType
from amplify_media_migrator.utils.media import (
    get_media_type,
    get_content_type,
    IMAGE_EXTENSIONS,
    VIDEO_EXTENSIONS,
    CONTENT_TYPES,
)


class TestGetMediaType:
    def test_image_extensions(self):
        for ext in IMAGE_EXTENSIONS:
            assert get_media_type(ext) == MediaType.IMAGE

    def test_video_extensions(self):
        for ext in VIDEO_EXTENSIONS:
            assert get_media_type(ext) == MediaType.VIDEO

    def test_case_insensitive(self):
        assert get_media_type("JPG") == MediaType.IMAGE
        assert get_media_type("Jpeg") == MediaType.IMAGE
        assert get_media_type("MP4") == MediaType.VIDEO
        assert get_media_type("MoV") == MediaType.VIDEO

    def test_strips_leading_dot(self):
        assert get_media_type(".jpg") == MediaType.IMAGE
        assert get_media_type(".mp4") == MediaType.VIDEO

    def test_strips_leading_dot_case_insensitive(self):
        assert get_media_type(".PNG") == MediaType.IMAGE
        assert get_media_type(".AVI") == MediaType.VIDEO

    def test_unknown_extension_raises_value_error(self):
        with pytest.raises(ValueError, match="Unknown extension"):
            get_media_type("pdf")

    def test_empty_extension_raises_value_error(self):
        with pytest.raises(ValueError, match="Unknown extension"):
            get_media_type("")


class TestGetContentType:
    def test_all_content_types(self):
        for ext, content_type in CONTENT_TYPES.items():
            assert get_content_type(ext) == content_type

    def test_case_insensitive(self):
        assert get_content_type("JPG") == "image/jpeg"
        assert get_content_type("JPEG") == "image/jpeg"
        assert get_content_type("PNG") == "image/png"
        assert get_content_type("GIF") == "image/gif"
        assert get_content_type("MP4") == "video/mp4"
        assert get_content_type("MOV") == "video/quicktime"
        assert get_content_type("AVI") == "video/x-msvideo"

    def test_strips_leading_dot(self):
        assert get_content_type(".jpg") == "image/jpeg"
        assert get_content_type(".mp4") == "video/mp4"

    def test_strips_leading_dot_case_insensitive(self):
        assert get_content_type(".PNG") == "image/png"
        assert get_content_type(".MOV") == "video/quicktime"

    def test_unknown_extension_raises_value_error(self):
        with pytest.raises(ValueError, match="Unknown extension"):
            get_content_type("pdf")

    def test_empty_extension_raises_value_error(self):
        with pytest.raises(ValueError, match="Unknown extension"):
            get_content_type("")
