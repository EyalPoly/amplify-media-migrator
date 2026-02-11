import pytest

from amplify_media_migrator.migration.mapper import (
    FilenameMapper,
    FilenamePattern,
)


@pytest.fixture
def mapper() -> FilenameMapper:
    return FilenameMapper()


class TestSinglePattern:
    def test_basic(self, mapper: FilenameMapper) -> None:
        result = mapper.parse("12345.jpg")
        assert result.pattern == FilenamePattern.SINGLE
        assert result.sequential_ids == [12345]
        assert result.extension == "jpg"
        assert result.error is None

    def test_various_extensions(self, mapper: FilenameMapper) -> None:
        for ext in ["jpg", "jpeg", "png", "gif", "mp4", "mov", "avi"]:
            result = mapper.parse(f"6602.{ext}")
            assert result.pattern == FilenamePattern.SINGLE
            assert result.sequential_ids == [6602]
            assert result.extension == ext

    def test_case_insensitive_extension(self, mapper: FilenameMapper) -> None:
        result = mapper.parse("6602.JPG")
        assert result.pattern == FilenamePattern.SINGLE
        assert result.sequential_ids == [6602]
        assert result.extension == "jpg"

    def test_single_digit(self, mapper: FilenameMapper) -> None:
        result = mapper.parse("1.png")
        assert result.pattern == FilenamePattern.SINGLE
        assert result.sequential_ids == [1]

    def test_large_number(self, mapper: FilenameMapper) -> None:
        result = mapper.parse("99999.mp4")
        assert result.pattern == FilenamePattern.SINGLE
        assert result.sequential_ids == [99999]


class TestMultiplePattern:
    def test_basic(self, mapper: FilenameMapper) -> None:
        result = mapper.parse("6602a.jpg")
        assert result.pattern == FilenamePattern.MULTIPLE
        assert result.sequential_ids == [6602]
        assert result.extension == "jpg"
        assert result.error is None

    def test_various_letters(self, mapper: FilenameMapper) -> None:
        for letter in ["a", "b", "c", "z"]:
            result = mapper.parse(f"1234{letter}.png")
            assert result.pattern == FilenamePattern.MULTIPLE
            assert result.sequential_ids == [1234]

    def test_case_insensitive_extension(self, mapper: FilenameMapper) -> None:
        result = mapper.parse("6602a.MOV")
        assert result.pattern == FilenamePattern.MULTIPLE
        assert result.sequential_ids == [6602]
        assert result.extension == "mov"


class TestRangePattern:
    def test_basic(self, mapper: FilenameMapper) -> None:
        result = mapper.parse("6000-6001.jpg")
        assert result.pattern == FilenamePattern.RANGE
        assert result.sequential_ids == [6000, 6001]
        assert result.extension == "jpg"
        assert result.error is None

    def test_larger_range(self, mapper: FilenameMapper) -> None:
        result = mapper.parse("1200-1205.mp4")
        assert result.pattern == FilenamePattern.RANGE
        assert result.sequential_ids == [1200, 1201, 1202, 1203, 1204, 1205]

    def test_same_start_end(self, mapper: FilenameMapper) -> None:
        result = mapper.parse("100-100.jpg")
        assert result.pattern == FilenamePattern.RANGE
        assert result.sequential_ids == [100]

    def test_reversed_range_is_invalid(self, mapper: FilenameMapper) -> None:
        result = mapper.parse("6001-6000.jpg")
        assert result.pattern == FilenamePattern.INVALID
        assert result.sequential_ids == []
        assert "greater than end" in (result.error or "")


class TestInvalidPattern:
    def test_non_numeric_base(self, mapper: FilenameMapper) -> None:
        result = mapper.parse("abc123.jpg")
        assert result.pattern == FilenamePattern.INVALID

    def test_text_filename(self, mapper: FilenameMapper) -> None:
        result = mapper.parse("photo.jpg")
        assert result.pattern == FilenamePattern.INVALID

    def test_missing_extension(self, mapper: FilenameMapper) -> None:
        result = mapper.parse("6602")
        assert result.pattern == FilenamePattern.INVALID
        assert "Missing file extension" in (result.error or "")

    def test_unsupported_extension(self, mapper: FilenameMapper) -> None:
        result = mapper.parse("6602.pdf")
        assert result.pattern == FilenamePattern.INVALID
        assert "Unsupported extension" in (result.error or "")

    def test_txt_extension(self, mapper: FilenameMapper) -> None:
        result = mapper.parse("6602.txt")
        assert result.pattern == FilenamePattern.INVALID

    def test_uppercase_letter_is_multiple(self, mapper: FilenameMapper) -> None:
        result = mapper.parse("6602A.jpg")
        assert result.pattern == FilenamePattern.MULTIPLE
        assert result.sequential_ids == [6602]

    def test_multiple_hyphens(self, mapper: FilenameMapper) -> None:
        result = mapper.parse("6000-6001-6002.jpg")
        assert result.pattern == FilenamePattern.INVALID

    def test_empty_string(self, mapper: FilenameMapper) -> None:
        result = mapper.parse("")
        assert result.pattern == FilenamePattern.INVALID


class TestIsValidExtension:
    def test_valid(self, mapper: FilenameMapper) -> None:
        for ext in ["jpg", "jpeg", "png", "gif", "mp4", "mov", "avi"]:
            assert mapper.is_valid_extension(ext) is True

    def test_with_dot(self, mapper: FilenameMapper) -> None:
        assert mapper.is_valid_extension(".jpg") is True

    def test_uppercase(self, mapper: FilenameMapper) -> None:
        assert mapper.is_valid_extension("JPG") is True

    def test_invalid(self, mapper: FilenameMapper) -> None:
        assert mapper.is_valid_extension("pdf") is False
        assert mapper.is_valid_extension("txt") is False


class TestBuildS3Key:
    def test_basic(self, mapper: FilenameMapper) -> None:
        key = mapper.build_s3_key("abc-123", "12345.jpg")
        assert key == "media/abc-123/12345.jpg"