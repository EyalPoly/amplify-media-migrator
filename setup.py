from pathlib import Path

from setuptools import setup, find_packages

_DEV_SECTION = "dev dependencies"


def _parse_requirements() -> tuple[list[str], list[str]]:
    runtime: list[str] = []
    dev: list[str] = []
    bucket = runtime
    for raw in (Path(__file__).parent / "requirements.txt").read_text().splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith("#"):
            if _DEV_SECTION in line.lower():
                bucket = dev
            continue
        bucket.append(line)
    return runtime, dev


requirements, dev_requirements = _parse_requirements()

setup(
    name="amplify-media-migrator",
    version="1.11.2",
    description="CLI tool to migrate media files from Google Drive to AWS Amplify Storage",
    author="MECO Team",
    python_requires=">=3.9",
    packages=find_packages(exclude=["tests", "tests.*"]),
    install_requires=requirements,
    extras_require={
        "dev": dev_requirements,
    },
    entry_points={
        "console_scripts": [
            "amplify-media-migrator=amplify_media_migrator.cli:main",
        ],
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
    ],
)
