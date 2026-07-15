import re
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional


class FilenamePattern(Enum):
    SINGLE = "single"
    MULTIPLE = "multiple"
    RANGE = "range"
    LIST = "list"
    INVALID = "invalid"


@dataclass
class ParsedFilename:
    pattern: FilenamePattern
    sequential_ids: List[int]
    extension: str
    original_filename: str
    error: Optional[str] = None
    prefix: str = ""


VALID_EXTENSIONS = {"jpg", "jpeg", "jfif", "png", "gif", "mp4", "mov", "avi", "wmv"}

COPY_SUFFIX_RE = re.compile(r"\s?\(\d+\)|\s-\s*copy", re.IGNORECASE)

_EXT = r"(jpg|jpeg|jfif|png|gif|mp4|mov|avi|wmv)"
_PREFIX = r"([A-Za-z]?)"
_SINGLE_RE = re.compile(rf"^{_PREFIX}(\d+)\.{_EXT}$", re.IGNORECASE)
_MULTIPLE_RE = re.compile(rf"^{_PREFIX}(\d+)[a-zA-Z]+\.{_EXT}$", re.IGNORECASE)
_MULTIPLE_SPACE_RE = re.compile(rf"^{_PREFIX}(\d+)\s+[a-zA-Z]+\.{_EXT}$", re.IGNORECASE)
_MULTIPLE_HYPHEN_RE = re.compile(
    rf"^{_PREFIX}(\d+)-[^-.]*[^-.\d][^-.]*\.{_EXT}$", re.IGNORECASE
)
_RANGE_RE = re.compile(rf"^{_PREFIX}(\d+)[-+](\d+)\.{_EXT}$", re.IGNORECASE)
_RANGE_MULTIPLE_RE = re.compile(
    rf"^{_PREFIX}(\d+)[-+](\d+)-[^-.]*[^-.\d][^-.]*\.{_EXT}$", re.IGNORECASE
)
_LIST_RE = re.compile(
    rf"^{_PREFIX}(?=[\dA-Za-z,+]*[A-Za-z])"
    rf"(\d+[A-Za-z]*(?:[,+]\d+[A-Za-z]*)+)\.{_EXT}$",
    re.IGNORECASE,
)


def _strip_copy_suffix(filename: str) -> str:
    dot_idx = filename.rfind(".")
    if dot_idx == -1:
        return COPY_SUFFIX_RE.sub("", filename)
    stem, ext = filename[:dot_idx], filename[dot_idx:]
    return COPY_SUFFIX_RE.sub("", stem) + ext


class FilenameMapper:
    VALID_EXTENSIONS = VALID_EXTENSIONS

    def parse(self, filename: str) -> ParsedFilename:
        name = _strip_copy_suffix(filename)

        for regex in (_RANGE_RE, _RANGE_MULTIPLE_RE):
            match = regex.match(name)
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

        list_match = _LIST_RE.match(name)
        if list_match:
            prefix = list_match.group(1)
            ids = [int(n) for n in re.findall(r"\d+", list_match.group(2))]
            ext = list_match.group(3).lower()
            return ParsedFilename(
                pattern=FilenamePattern.LIST,
                sequential_ids=ids,
                extension=ext,
                original_filename=filename,
                prefix=prefix,
            )

        for regex, pattern in (
            (_MULTIPLE_HYPHEN_RE, FilenamePattern.MULTIPLE),
            (_MULTIPLE_SPACE_RE, FilenamePattern.MULTIPLE),
            (_MULTIPLE_RE, FilenamePattern.MULTIPLE),
            (_SINGLE_RE, FilenamePattern.SINGLE),
        ):
            match = regex.match(name)
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
