from enum import Enum


class MediaType(Enum):
    IMAGE = "IMAGE"
    VIDEO = "VIDEO"


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


def _normalize_extension(extension: str) -> str:
    return extension.lower().lstrip(".")


def get_media_type(extension: str) -> MediaType:
    ext = _normalize_extension(extension)
    if ext in IMAGE_EXTENSIONS:
        return MediaType.IMAGE
    if ext in VIDEO_EXTENSIONS:
        return MediaType.VIDEO
    raise ValueError(f"Unknown extension: {extension}")


def get_content_type(extension: str) -> str:
    ext = _normalize_extension(extension)
    if ext not in CONTENT_TYPES:
        raise ValueError(f"Unknown extension: {extension}")
    return CONTENT_TYPES[ext]