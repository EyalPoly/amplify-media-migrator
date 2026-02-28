from setuptools import setup, find_packages

requirements = [
    "amplify-auth>=0.1.0",
    "click>=8.1.0",
    "boto3>=1.34.0",
    "pycognito>=2023.5.0",
    "PyJWT>=2.0.0",
    "google-api-python-client>=2.0.0",
    "google-auth-httplib2>=0.2.0",
    "google-auth-oauthlib>=1.0.0",
    "gql[requests]>=3.5.0",
    "requests>=2.31.0",
    "python-dateutil>=2.8.0",
    "rich>=13.7.0",
    "tqdm>=4.66.0",
]

dev_requirements = [
    "pytest>=7.4.0",
    "pytest-cov>=4.1.0",
    "pytest-asyncio>=0.21.0",
    "mypy>=1.8.0",
    "black>=24.1.0",
    "moto>=4.2.0",
]

setup(
    name="amplify-media-migrator",
    version="1.0.4",
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
