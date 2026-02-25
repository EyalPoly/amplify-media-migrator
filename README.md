# amplify-media-migrator

CLI tool to migrate media files (photos/videos) from Google Drive to AWS Amplify Storage and link them to existing Observation records in the MECO database.

## Features

- Recursive Google Drive folder scanning with filename pattern validation
- Parallel file download/upload with configurable concurrency
- Automatic filename-to-observation mapping (single, multiple, range patterns)
- S3 upload via Cognito Identity Pool credentials
- GraphQL Media record creation linked to Observations
- Resumable migrations with persistent progress tracking
- Built-in rate limiting for Google Drive API
- Retry with exponential backoff for transient errors
- Export and review tools for problematic files

## Prerequisites

- Python 3.9+
- Google Cloud project with Drive API enabled
- AWS account with Amplify backend deployed (S3 bucket, AppSync API, Cognito)
- Cognito user in the ADMINS group

## Installation

```bash
pip install amplify-media-migrator
```

## Google Drive Setup

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a project (or use existing) and enable the **Google Drive API**
3. Create **OAuth 2.0 credentials** (Desktop application type)
4. Download the credentials JSON file
5. Place it at `~/.amplify-media-migrator/google_credentials.json`

On first run, a browser window will open for OAuth consent. The resulting token is saved to `~/.amplify-media-migrator/google_token.json`.

## AWS Setup

Ensure the following are deployed and available:

| Resource | Description |
|---|---|
| **S3 Bucket** | Amplify Storage bucket for media files |
| **AppSync API** | GraphQL endpoint with Observation and Media models |
| **Cognito User Pool** | User pool with ADMINS group |
| **Cognito Identity Pool** | Federated identity pool for S3 access |

## Configuration

Run the interactive setup:

```bash
amplify-media-migrator config
```

This creates `~/.amplify-media-migrator/config.json` with:

- Google Drive folder ID, credentials path, and token path
- AWS region, Cognito user pool/client/identity pool IDs, username
- AppSync API endpoint and S3 bucket name
- Migration settings (concurrency, retries, chunk size)

View current configuration:

```bash
amplify-media-migrator show
```

## Usage Workflow

### 1. Configure

```bash
amplify-media-migrator config
```

### 2. Validate

Run pre-flight checks to verify all services are accessible:

```bash
amplify-media-migrator validate --folder-id FOLDER_ID
```

Checks: config, Google Drive auth, folder access, Cognito auth, S3 bucket, GraphQL endpoint.

### 3. Scan

Dry-run scan to validate file naming patterns without uploading:

```bash
amplify-media-migrator scan --folder-id FOLDER_ID
```

### 4. Review

Inspect files with invalid naming patterns:

```bash
amplify-media-migrator review --folder-id FOLDER_ID
```

Rename problematic files in Google Drive, then re-scan.

### 5. Migrate

Run the full migration:

```bash
amplify-media-migrator migrate --folder-id FOLDER_ID
```

Options:

| Flag | Default | Description |
|---|---|---|
| `--concurrency` | 10 | Number of parallel workers |
| `--dry-run` | off | Validate without uploading |
| `--skip-existing` | off | Skip files with existing Media records |
| `--verbose` | off | Enable debug logging |

### 6. Check Status

View migration progress (no auth required):

```bash
amplify-media-migrator status --folder-id FOLDER_ID
```

### 7. Resume

Resume an interrupted migration (retries failed/pending files):

```bash
amplify-media-migrator resume --folder-id FOLDER_ID
```

### 8. Export

Export files by status for offline review:

```bash
amplify-media-migrator export --folder-id FOLDER_ID --status orphan --output orphans.json
amplify-media-migrator export --folder-id FOLDER_ID --status needs_review --output review.json
```

## File Naming Conventions

| Pattern | Example | Mapping |
|---|---|---|
| Single | `12345.jpg` | 1 file -> 1 observation (sequentialId=12345) |
| Multiple | `6602a.jpg` | Multiple files -> 1 observation (sequentialId=6602) |
| Range | `6000-6001.jpg` | 1 file -> multiple observations (6000, 6001) |

**Valid extensions**: jpg, jpeg, png, gif, mp4, mov, avi (case-insensitive)

Files that don't match any pattern are marked as `needs_review`.

## CLI Reference

| Command | Description |
|---|---|
| `config` | Interactive configuration setup |
| `show` | Display current configuration |
| `validate --folder-id ID` | Pre-flight service connectivity checks |
| `scan --folder-id ID` | Scan folder and validate file patterns |
| `review --folder-id ID` | Show files needing manual review |
| `migrate --folder-id ID` | Run full migration |
| `resume --folder-id ID` | Resume interrupted migration |
| `status --folder-id ID` | Show migration progress |
| `export --folder-id ID --status STATUS --output FILE` | Export files by status |

## Troubleshooting

| Issue | Solution |
|---|---|
| "Token expired" | Delete `~/.amplify-media-migrator/google_token.json` and re-authenticate |
| "Access denied to Drive folder" | Ensure the folder is shared with your OAuth account |
| "User not in ADMINS group" | Add the user to ADMINS in the Cognito console |
| "S3 bucket not found" | Deploy Amplify storage first |
| "Observation not found" | Verify the sequentialId exists, check the filename pattern |
| "Rate limit exceeded" | Reduce `--concurrency` |

## Development

```bash
git clone <repo-url>
cd amplify-media-migrator
python -m venv venv
source venv/bin/activate
pip install -e ".[dev]"

# Run tests
pytest

# Run tests with coverage
pytest --cov=amplify_media_migrator

# Type checking
mypy amplify_media_migrator

# Code formatting
black amplify_media_migrator tests
```

## License

Private - MECO Project
