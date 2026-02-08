#!/usr/bin/env python3
"""Script to automatically bump patch version in setup.py."""

import os
import re
import sys


def get_current_version(setup_file: str = "setup.py") -> str:
    """Read the current version from setup.py."""
    with open(setup_file, "r") as f:
        content = f.read()

    match = re.search(r'version\s*=\s*["\']([0-9]+\.[0-9]+\.[0-9]+)["\']', content)
    if not match:
        raise ValueError("Could not find version in setup.py")

    return match.group(1)


def bump_patch_version(version: str) -> str:
    """Increment the patch version (e.g., 0.1.0 -> 0.1.1)."""
    major, minor, patch = version.split(".")
    new_patch = int(patch) + 1
    return f"{major}.{minor}.{new_patch}"


def update_version_in_file(setup_file: str, old_version: str, new_version: str) -> str:
    """Update the version string in setup.py."""
    with open(setup_file, "r") as f:
        content = f.read()

    pattern = rf'version\s*=\s*["\']({re.escape(old_version)})["\']'
    new_content = re.sub(pattern, f'version="{new_version}"', content)

    with open(setup_file, "w") as f:
        f.write(new_content)

    return new_version


def write_github_output(version: str, old_version: str) -> None:
    """Write version info to GITHUB_OUTPUT if running in GitHub Actions."""
    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as f:
            f.write(f"version={version}\n")
            f.write(f"old_version={old_version}\n")


def main() -> int:
    setup_file = "setup.py"

    try:
        current_version = get_current_version(setup_file)
        print(f"Current version: {current_version}")

        new_version = bump_patch_version(current_version)
        print(f"New version: {new_version}")

        update_version_in_file(setup_file, current_version, new_version)
        print(f"Updated {setup_file}")

        write_github_output(new_version, current_version)

        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())