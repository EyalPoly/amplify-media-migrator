from dataclasses import dataclass
from enum import Enum
from typing import Optional, List


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


class FilenameMapper:
    VALID_EXTENSIONS = {"jpg", "jpeg", "png", "gif", "mp4", "mov", "avi"}

    def parse(self, filename: str) -> ParsedFilename:
        raise NotImplementedError

    def is_valid_extension(self, extension: str) -> bool:
        return extension.lower() in self.VALID_EXTENSIONS

    def build_s3_key(self, observation_id: str, filename: str) -> str:
        raise NotImplementedError
