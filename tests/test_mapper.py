import pytest

from amplify_media_migrator.migration.mapper import (
    FilenameMapper,
    FilenamePattern,
    ParsedFilename,
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

    def test_multi_letter_suffix(self, mapper: FilenameMapper) -> None:
        for suffix in ["aa", "ab", "as", "ah", "aaa"]:
            result = mapper.parse(f"3826{suffix}.jpg")
            assert result.pattern == FilenamePattern.MULTIPLE
            assert result.sequential_ids == [3826]
            assert result.extension == "jpg"
            assert result.error is None

    def test_multi_letter_suffix_video(self, mapper: FilenameMapper) -> None:
        result = mapper.parse("3571aa.mp4")
        assert result.pattern == FilenamePattern.MULTIPLE
        assert result.sequential_ids == [3571]
        assert result.extension == "mp4"

    def test_hyphen_letter_uppercase(self, mapper: FilenameMapper) -> None:
        result = mapper.parse("131-A.jpg")
        assert result.pattern == FilenamePattern.MULTIPLE
        assert result.sequential_ids == [131]
        assert result.extension == "jpg"
        assert result.error is None

    def test_hyphen_letter_lowercase(self, mapper: FilenameMapper) -> None:
        result = mapper.parse("131-a.jpg")
        assert result.pattern == FilenamePattern.MULTIPLE
        assert result.sequential_ids == [131]

    def test_hyphen_letter_various(self, mapper: FilenameMapper) -> None:
        for letter in ["A", "B", "Z", "a", "b", "z"]:
            result = mapper.parse(f"6602-{letter}.png")
            assert result.pattern == FilenamePattern.MULTIPLE
            assert result.sequential_ids == [6602]

    def test_hyphen_letter_case_insensitive_extension(
        self, mapper: FilenameMapper
    ) -> None:
        result = mapper.parse("131-A.MOV")
        assert result.pattern == FilenamePattern.MULTIPLE
        assert result.sequential_ids == [131]
        assert result.extension == "mov"

    def test_hyphen_double_letter(self, mapper: FilenameMapper) -> None:
        for label in ["AA", "AB", "AC", "DD", "AN"]:
            result = mapper.parse(f"1550-{label}.jpg")
            assert result.pattern == FilenamePattern.MULTIPLE
            assert result.sequential_ids == [1550]
            assert result.extension == "jpg"

    def test_hyphen_letter_with_drive_duplicate_suffix(
        self, mapper: FilenameMapper
    ) -> None:
        result = mapper.parse("1540-A (1).jpg")
        assert result.pattern == FilenamePattern.MULTIPLE
        assert result.sequential_ids == [1540]
        assert result.extension == "jpg"
        assert result.error is None

    def test_hyphen_non_ascii_letter(self, mapper: FilenameMapper) -> None:
        result = mapper.parse("1885-ק.jpg")
        assert result.pattern == FilenamePattern.MULTIPLE
        assert result.sequential_ids == [1885]
        assert result.extension == "jpg"


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


class TestRangeWithLetterSuffix:
    def test_basic_uppercase(self, mapper: FilenameMapper) -> None:
        result = mapper.parse("2503-2504-A.jpg")
        assert result.pattern == FilenamePattern.RANGE
        assert result.sequential_ids == [2503, 2504]
        assert result.extension == "jpg"
        assert result.error is None

    def test_siblings_share_ids(self, mapper: FilenameMapper) -> None:
        for label in ["A", "B", "C"]:
            result = mapper.parse(f"2606-2607-{label}.mp4")
            assert result.pattern == FilenamePattern.RANGE
            assert result.sequential_ids == [2606, 2607]
            assert result.extension == "mp4"

    def test_lowercase_suffix(self, mapper: FilenameMapper) -> None:
        result = mapper.parse("2503-2504-b.jpg")
        assert result.pattern == FilenamePattern.RANGE
        assert result.sequential_ids == [2503, 2504]

    def test_larger_range(self, mapper: FilenameMapper) -> None:
        result = mapper.parse("1200-1202-A.mp4")
        assert result.pattern == FilenamePattern.RANGE
        assert result.sequential_ids == [1200, 1201, 1202]

    def test_with_prefix(self, mapper: FilenameMapper) -> None:
        result = mapper.parse("E6000-6001-A.jpg")
        assert result.pattern == FilenamePattern.RANGE
        assert result.sequential_ids == [6000, 6001]
        assert result.prefix == "E"

    def test_preserves_original(self, mapper: FilenameMapper) -> None:
        result = mapper.parse("2503-2504-A.jpg")
        assert result.original_filename == "2503-2504-A.jpg"

    def test_reversed_is_invalid(self, mapper: FilenameMapper) -> None:
        result = mapper.parse("2504-2503-A.jpg")
        assert result.pattern == FilenamePattern.INVALID
        assert "greater than end" in (result.error or "")

    def test_numeric_third_segment_still_invalid(self, mapper: FilenameMapper) -> None:
        result = mapper.parse("6000-6001-6002.jpg")
        assert result.pattern == FilenamePattern.INVALID


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


class TestJfifExtension:
    def test_single(self, mapper: FilenameMapper) -> None:
        result = mapper.parse("4290.jfif")
        assert result.pattern == FilenamePattern.SINGLE
        assert result.sequential_ids == [4290]
        assert result.extension == "jfif"
        assert result.error is None

    def test_multiple(self, mapper: FilenameMapper) -> None:
        result = mapper.parse("4328a.jfif")
        assert result.pattern == FilenamePattern.MULTIPLE
        assert result.sequential_ids == [4328]
        assert result.extension == "jfif"

    def test_range(self, mapper: FilenameMapper) -> None:
        result = mapper.parse("6000-6001.jfif")
        assert result.pattern == FilenamePattern.RANGE
        assert result.sequential_ids == [6000, 6001]
        assert result.extension == "jfif"

    def test_case_insensitive(self, mapper: FilenameMapper) -> None:
        result = mapper.parse("4290.JFIF")
        assert result.pattern == FilenamePattern.SINGLE
        assert result.extension == "jfif"


class TestPlusRangeSeparator:
    def test_basic(self, mapper: FilenameMapper) -> None:
        result = mapper.parse("4394+4395.mp4")
        assert result.pattern == FilenamePattern.RANGE
        assert result.sequential_ids == [4394, 4395]
        assert result.extension == "mp4"
        assert result.error is None

    def test_larger_range(self, mapper: FilenameMapper) -> None:
        result = mapper.parse("1200+1205.jpg")
        assert result.pattern == FilenamePattern.RANGE
        assert result.sequential_ids == [1200, 1201, 1202, 1203, 1204, 1205]

    def test_reversed_is_invalid(self, mapper: FilenameMapper) -> None:
        result = mapper.parse("4395+4394.mp4")
        assert result.pattern == FilenamePattern.INVALID
        assert "greater than end" in (result.error or "")

    def test_with_letter_suffix(self, mapper: FilenameMapper) -> None:
        result = mapper.parse("2503+2504-A.jpg")
        assert result.pattern == FilenamePattern.RANGE
        assert result.sequential_ids == [2503, 2504]


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


class TestOriginalFilename:
    def test_single_preserves_original(self, mapper: FilenameMapper) -> None:
        result = mapper.parse("6602.JPG")
        assert result.original_filename == "6602.JPG"

    def test_multiple_preserves_original(self, mapper: FilenameMapper) -> None:
        result = mapper.parse("6602a.MOV")
        assert result.original_filename == "6602a.MOV"

    def test_range_preserves_original(self, mapper: FilenameMapper) -> None:
        result = mapper.parse("6000-6001.jpg")
        assert result.original_filename == "6000-6001.jpg"

    def test_invalid_preserves_original(self, mapper: FilenameMapper) -> None:
        result = mapper.parse("bad_file.pdf")
        assert result.original_filename == "bad_file.pdf"


class TestInvalidErrorMessages:
    def test_valid_ext_invalid_name(self, mapper: FilenameMapper) -> None:
        result = mapper.parse("photo.jpg")
        assert result.pattern == FilenamePattern.INVALID
        assert result.error == "Filename does not match any valid pattern"
        assert result.extension == "jpg"

    def test_no_extension_error(self, mapper: FilenameMapper) -> None:
        result = mapper.parse("noext")
        assert result.error == "Missing file extension"
        assert result.extension == ""

    def test_unsupported_ext_error(self, mapper: FilenameMapper) -> None:
        result = mapper.parse("6602.bmp")
        assert result.error == "Unsupported extension: bmp"
        assert result.extension == "bmp"


class TestParsedFilenameDataclass:
    def test_default_error_is_none(self) -> None:
        pf = ParsedFilename(
            pattern=FilenamePattern.SINGLE,
            sequential_ids=[1],
            extension="jpg",
            original_filename="1.jpg",
        )
        assert pf.error is None

    def test_all_fields_set(self) -> None:
        pf = ParsedFilename(
            pattern=FilenamePattern.INVALID,
            sequential_ids=[],
            extension="txt",
            original_filename="bad.txt",
            error="Unsupported extension: txt",
        )
        assert pf.pattern == FilenamePattern.INVALID
        assert pf.sequential_ids == []
        assert pf.extension == "txt"
        assert pf.original_filename == "bad.txt"
        assert pf.error == "Unsupported extension: txt"


class TestBuildS3Key:
    def test_basic(self, mapper: FilenameMapper) -> None:
        key = mapper.build_s3_key("abc-123", "12345.jpg")
        assert key == "media/abc-123/12345.jpg"

    def test_range_filename(self, mapper: FilenameMapper) -> None:
        key = mapper.build_s3_key("obs-abc", "6000-6001.jpg")
        assert key == "media/obs-abc/6000-6001.jpg"

    def test_multiple_filename(self, mapper: FilenameMapper) -> None:
        key = mapper.build_s3_key("obs-def", "6602a.jpg")
        assert key == "media/obs-def/6602a.jpg"


class TestPrefix:
    def test_no_prefix(self, mapper: FilenameMapper) -> None:
        result = mapper.parse("5.jpg")
        assert result.pattern == FilenamePattern.SINGLE
        assert result.sequential_ids == [5]
        assert result.prefix == ""

    def test_letter_prefix_single(self, mapper: FilenameMapper) -> None:
        result = mapper.parse("E5.jpg")
        assert result.pattern == FilenamePattern.SINGLE
        assert result.sequential_ids == [5]
        assert result.prefix == "E"

    def test_prefix_preserves_case(self, mapper: FilenameMapper) -> None:
        assert mapper.parse("s5.jpg").prefix == "s"

    def test_prefix_on_range(self, mapper: FilenameMapper) -> None:
        result = mapper.parse("E6000-6005.jpg")
        assert result.pattern == FilenamePattern.RANGE
        assert result.sequential_ids == [6000, 6001, 6002, 6003, 6004, 6005]
        assert result.prefix == "E"

    def test_prefix_on_multiple(self, mapper: FilenameMapper) -> None:
        result = mapper.parse("S6000a.jpg")
        assert result.pattern == FilenamePattern.MULTIPLE
        assert result.sequential_ids == [6000]
        assert result.prefix == "S"

    def test_multiletter_name_still_invalid(self, mapper: FilenameMapper) -> None:
        # only a single letter immediately followed by digits is a prefix
        result = mapper.parse("final2.jpg")
        assert result.pattern == FilenamePattern.INVALID
