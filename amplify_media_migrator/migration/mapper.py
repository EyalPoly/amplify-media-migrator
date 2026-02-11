import re
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional


class FilenamePattern(Enum):
    SINGLE = "single"
    MULTIPLE = "multiple"
    RANGE = "range"
    INVALID = "invalid"


@dataclass
class ParsedFilename:
    pattern: FilenamePattern
    sequential_ids: List[int]
    extension: str
    original_filename: str
    error: Optional[str] = None


VALID_EXTENSIONS = {"jpg", "jpeg", "png", "gif", "mp4", "mov", "avi"}

_SINGLE_RE = re.compile(r"^(\d+)\.(jpg|jpeg|png|gif|mp4|mov|avi)$", re.IGNORECASE)
_MULTIPLE_RE = re.compile(
    r"^(\d+)[a-zA-Z]\.(jpg|jpeg|png|gif|mp4|mov|avi)$", re.IGNORECASE
)
_RANGE_RE = re.compile(
    r"^(\d+)-(\d+)\.(jpg|jpeg|png|gif|mp4|mov|avi)$", re.IGNORECASE
)


class FilenameMapper:
    VALID_EXTENSIONS = VALID_EXTENSIONS

    def parse(self, filename: str) -> ParsedFilename:
        match = _RANGE_RE.match(filename)
        if match:
            start, end = int(match.group(1)), int(match.group(2))
            ext = match.group(3).lower()
            if start > end:
                return ParsedFilename(
                    pattern=FilenamePattern.INVALID,
                    sequential_ids=[],
                    extension=ext,
                    original_filename=filename,
                    error=f"Range start ({start}) is greater than end ({end})",
                )
            ids = list(range(start, end + 1))
            return ParsedFilename(
                pattern=FilenamePattern.RANGE,
                sequential_ids=ids,
                extension=ext,
                original_filename=filename,
            )

        match = _MULTIPLE_RE.match(filename)
        if match:
            seq_id = int(match.group(1))
            ext = match.group(2).lower()
            return ParsedFilename(
                pattern=FilenamePattern.MULTIPLE,
                sequential_ids=[seq_id],
                extension=ext,
                original_filename=filename,
            )

        match = _SINGLE_RE.match(filename)
        if match:
            seq_id = int(match.group(1))
            ext = match.group(2).lower()
            return ParsedFilename(
                pattern=FilenamePattern.SINGLE,
                sequential_ids=[seq_id],
                extension=ext,
                original_filename=filename,
            )

        dot_idx = filename.rfind(".")
        ext = filename[dot_idx + 1 :].lower() if dot_idx != -1 else ""

        if ext and ext not in VALID_EXTENSIONS:
            error = f"Unsupported extension: {ext}"
        elif not ext:
            error = "Missing file extension"
        else:
            error = "Filename does not match any valid pattern"

        return ParsedFilename(
            pattern=FilenamePattern.INVALID,
            sequential_ids=[],
            extension=ext,
            original_filename=filename,
            error=error,
        )

    def is_valid_extension(self, extension: str) -> bool:
        return extension.lower().lstrip(".") in VALID_EXTENSIONS

    def build_s3_key(self, observation_id: str, filename: str) -> str:
        return f"media/{observation_id}/{filename}"