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
    prefix: str = ""


VALID_EXTENSIONS = {"jpg", "jpeg", "png", "gif", "mp4", "mov", "avi", "wmv"}

_EXT = r"(jpg|jpeg|png|gif|mp4|mov|avi|wmv)"
_PREFIX = r"([A-Za-z]?)"
_SINGLE_RE = re.compile(rf"^{_PREFIX}(\d+)\.{_EXT}$", re.IGNORECASE)
_MULTIPLE_RE = re.compile(rf"^{_PREFIX}(\d+)[a-zA-Z]+\.{_EXT}$", re.IGNORECASE)
_MULTIPLE_HYPHEN_RE = re.compile(
    rf"^{_PREFIX}(\d+)-[^-.]*[^-.\d][^-.]*\.{_EXT}$", re.IGNORECASE
)
_RANGE_RE = re.compile(rf"^{_PREFIX}(\d+)-(\d+)\.{_EXT}$", re.IGNORECASE)
_RANGE_MULTIPLE_RE = re.compile(
    rf"^{_PREFIX}(\d+)-(\d+)-[^-.]*[^-.\d][^-.]*\.{_EXT}$", re.IGNORECASE
)


class FilenameMapper:
    VALID_EXTENSIONS = VALID_EXTENSIONS

    def parse(self, filename: str) -> ParsedFilename:
        for regex in (_RANGE_RE, _RANGE_MULTIPLE_RE):
            match = regex.match(filename)
            if match:
                prefix = match.group(1)
                start, end = int(match.group(2)), int(match.group(3))
                ext = match.group(4).lower()
                if start > end:
                    return ParsedFilename(
                        pattern=FilenamePattern.INVALID,
                        sequential_ids=[],
                        extension=ext,
                        original_filename=filename,
                        error=f"Range start ({start}) is greater than end ({end})",
                        prefix=prefix,
                    )
                return ParsedFilename(
                    pattern=FilenamePattern.RANGE,
                    sequential_ids=list(range(start, end + 1)),
                    extension=ext,
                    original_filename=filename,
                    prefix=prefix,
                )

        for regex, pattern in (
            (_MULTIPLE_HYPHEN_RE, FilenamePattern.MULTIPLE),
            (_MULTIPLE_RE, FilenamePattern.MULTIPLE),
            (_SINGLE_RE, FilenamePattern.SINGLE),
        ):
            match = regex.match(filename)
            if match:
                prefix = match.group(1)
                seq_id = int(match.group(2))
                ext = match.group(3).lower()
                return ParsedFilename(
                    pattern=pattern,
                    sequential_ids=[seq_id],
                    extension=ext,
                    original_filename=filename,
                    prefix=prefix,
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
