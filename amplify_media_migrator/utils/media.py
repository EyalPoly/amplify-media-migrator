from ..targets.graphql_client import MediaType


IMAGE_EXTENSIONS = {"jpg", "jpeg", "png", "gif"}
VIDEO_EXTENSIONS = {"mp4", "mov", "avi"}

CONTENT_TYPES = {
    "jpg": "image/jpeg",
    "jpeg": "image/jpeg",
    "png": "image/png",
    "gif": "image/gif",
    "mp4": "video/mp4",
    "mov": "video/quicktime",
    "avi": "video/x-msvideo",
}


def get_media_type(extension: str) -> MediaType:
    raise NotImplementedError


def get_content_type(extension: str) -> str:
    raise NotImplementedError