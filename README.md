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
2. Select an existing project or create a new one
3. Navigate to **APIs & Services > Enabled APIs & services**
4. Click **+ ENABLE APIS AND SERVICES**, search for **Google Drive API**, and enable it
5. Go to **APIs & Services > Credentials**
6. Click **+ CREATE CREDENTIALS > OAuth client ID**
7. If prompted to configure the consent screen first:
   - Choose **External** user type
   - Fill in an app name (e.g. "Media Migrator") and your email
   - Skip scopes, add your email as a test user, then save
8. Back on Create OAuth client ID: select **Desktop app**, give it a name, click **Create**
9. Click **Download JSON** on the confirmation dialog
10. Move the downloaded file:
    ```bash
    mv ~/Downloads/client_secret_*.json ~/.amplify-media-migrator/google_credentials.json
    ```

On first run, a browser window will open for OAuth consent. The resulting token is saved to `~/.amplify-media-migrator/google_token.json`.

## AWS Setup

The Amplify backend must already be deployed. You need the following values from it:

### S3 Bucket Name

1. Go to the [AWS S3 Console](https://s3.console.aws.amazon.com/s3/buckets)
2. Look for a bucket matching `amplify-<app-id>-<env>-<hash>-<storage-name>`
3. Or run: `aws s3 ls | grep meco`

### AppSync API Endpoint

1. Go to the [AWS AppSync Console](https://console.aws.amazon.com/appsync)
2. Select your API and copy the **API URL** from the Settings page

### Cognito User Pool & Identity Pool

1. Go to the [Amazon Cognito Console](https://console.aws.amazon.com/cognito)
2. Select your **User Pool** — copy the **User Pool ID** and **App client ID** from the App integration tab
3. Select your **Identity Pool** — copy the **Identity Pool ID**
4. Ensure your user is in the **ADMINS** group

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

### Google Drive Folder ID

The `--folder-id` used throughout the CLI is the ID from the Google Drive folder URL:

```
https://drive.google.com/drive/folders/1ABCxyz123456789
                                       └── this is the folder ID
```

Open the folder containing your media files in Google Drive and copy the ID from the URL bar.

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

Run the migration. The command is safe to re-run — completed files are skipped
and previously failed files are retried automatically:

```bash
amplify-media-migrator migrate --folder-id FOLDER_ID
```

The first run scans the Drive folder and queues every valid file. Later runs
rebuild the work list from the saved progress file (no folder re-scan) and
retry any `failed` / `partial` / interrupted files. To re-scan the folder and
pick up files added to Drive since the first run:

```bash
amplify-media-migrator migrate --folder-id FOLDER_ID --rescan
```

Orphan files (no matching Observation) are skipped by default. To re-attempt
them after the Observations exist in the database:

```bash
amplify-media-migrator migrate --folder-id FOLDER_ID --retry-orphans
```

Use `--dry-run` to validate without downloading or uploading.

#### Keeping the machine awake

`migrate` automatically prevents your machine from sleeping for the duration of
the run, so a long migration keeps going when the screen turns off:

- **macOS**: via `caffeinate` (covers display, idle, disk, and system sleep).
- **Windows**: via `SetThreadExecutionState`.
- **Linux**: via `systemd-inhibit` (requires systemd).

This is always on and built in — there is no flag or config to disable it. If
the mechanism is unavailable, migration logs a warning and runs anyway;
progress autosaves and the run is resumable regardless.

> **macOS clamshell caveat:** `caffeinate` cannot stop sleep when the laptop lid
> is physically closed. Keep the lid open (the screen may turn off) for an
> unattended run, or set `sudo pmset -c disablesleep 1` yourself (not done
> automatically).

### 6. Check Status

View migration progress (no auth required):

```bash
amplify-media-migrator status --folder-id FOLDER_ID
```

### 7. Export

Export files by status for offline review:

```bash
amplify-media-migrator export --folder-id FOLDER_ID --status orphan --output orphans.json
amplify-media-migrator export --folder-id FOLDER_ID --status needs_review --output review.json
amplify-media-migrator export --folder-id FOLDER_ID --status duplicate --output duplicates.json
```

Valid `--status` values: `failed`, `orphan`, `needs_review`, `partial`, `duplicate`.

### Deduplication

Before processing, the migrator skips byte-identical copies: files sharing the
same Google Drive md5 checksum **and** the same sequential IDs are collapsed to a
single upload. The survivor is chosen deterministically (an already-completed
file wins; otherwise the cleanest filename — no ` (1)` / ` - Copy` suffix), and
the skipped copies are recorded with the `duplicate` status. No extra downloads
are performed; checksums come from Drive metadata.

## File Naming Conventions

| Pattern | Example | Mapping |
|---|---|---|
| Single | `12345.jpg` | 1 file -> 1 observation (sequentialId=12345) |
| Multiple | `6602a.jpg` | Multiple files -> 1 observation (sequentialId=6602) |
| Range | `6000-6001.jpg` | 1 file -> multiple observations (6000, 6001) |

**Valid extensions**: jpg, jpeg, png, gif, mp4, mov, avi (case-insensitive)

Files that don't match any pattern are marked as `needs_review`.

### Prefix-based disambiguation (reused sequentialId)

When `sequentialId` is reused across datasets, a single-letter filename prefix selects which
observation a media file links to, by matching a configurable field. Configure it with
`amplify-media-migrator config` (answer yes to "Enable prefix-based observation disambiguation"),
or edit `~/.amplify-media-migrator/config.json`:

```json
{
  "prefix_disambiguation": {
    "enabled": true,
    "discriminator_field": "countryId",
    "prefixes": { "": "<value for no-prefix files>", "E": "<value>", "S": "*" }
  }
}
```

- `discriminator_field` — any Observation field/FK that tells reused-id records apart.
- `prefixes` — maps each filename prefix (`""` = no prefix) to the value that field must equal;
  `"*"` is a catch-all meaning "any value not listed by another prefix".

The config is global and sticky: every `migrate` run applies it until you change or disable it.
Files are never renamed — the prefix is read, not changed, and the original filename is kept in the
S3 key. A prefix not present in `prefixes` produces an orphan (visible), never a silent mis-link.
Only a single leading letter directly followed by digits is treated as a prefix (`E5.jpg`); names
like `final2.jpg` are unaffected. When `enabled` is false, linking uses the first observation found
for a `sequentialId` (legacy behaviour).

## CLI Reference

| Command | Description |
|---|---|
| `config` | Interactive configuration setup |
| `show` | Display current configuration |
| `validate --folder-id ID` | Pre-flight service connectivity checks |
| `scan --folder-id ID` | Scan folder and validate file patterns |
| `review --folder-id ID` | Show files needing manual review |
| `migrate --folder-id ID [--rescan] [--retry-orphans] [--dry-run]` | Run or resume migration. Safe to re-run; retries failures automatically. `--rescan` re-lists Drive for new files; `--retry-orphans` re-processes orphan files |
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
| "Rate limit exceeded" | Reduce `migration.max_workers` in config |

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
