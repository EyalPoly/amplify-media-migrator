# amplify-media-migrator

CLI tool to migrate ~20,000 media files (photos/videos) from Google Drive to AWS Amplify Storage and link them to existing Observation records in the MECO database.

## Project Context

### Related Projects
- **MECO App**: https://github.com/sworgkh/meco-observations-amplify (Amplify Gen 2)
- **Excel Migrator**: https://github.com/EyalPoly/amplify-excel-migrator (reference for auth/CLI patterns)

### Scale & Performance
- **Files**: ~20,000 (mix of photos and videos)
- **Total size**: ~300 GB estimated (avg 15 MB/file)
- **Expected duration**: 4-8 hours with local execution
- **Bottleneck**: Google Drive API rate limit (10 queries/sec sustained)

---

## Architecture

### Data Flow
```
1. Google Drive API → Recursively list all files in folder and subdirectories
2. For each file:
   a. Parse filename to determine pattern type:
      - Single: "6602.jpg" → [6602]
      - Multiple: "6602a.jpg" → [6602] (same as base)
      - Range: "6000-6001.jpg" → [6000, 6001]
      - Invalid: Mark as "needs_review", skip to next file

   b. For each sequentialId extracted:
      - Query Observation by sequentialId → get observation.id
      - If not found: Mark as orphan for this sequentialId

   c. If at least one observation found:
      - Download file from Google Drive (once, regardless of # observations)
      - Upload to S3 using first observation.id in key (once)
      - For each valid observation.id:
        * Create Media record with observationId and S3 URL

   d. Update progress tracker:
      - completed: All observations linked successfully
      - partial: Some observations linked, some orphaned
      - orphan: No observations found
      - failed: Retryable error occurred
      - needs_review: Invalid filename pattern
```

### Folder Structure
Files are organized in nested subdirectories within the main Google Drive folder:
- **Format**: Range folders like `1-500`, `1000-1400`, `5000-5999`, etc.
- **Purpose**: Organization to avoid single folder with 20,000+ files
- **Migration**: Must recursively traverse all subdirectories

### File Naming Convention
Files follow these patterns:

#### Pattern 1: Single media per observation (most common)
- **Format**: `{sequentialId}.{extension}`
- **Examples**:
  - `12345.jpg` → sequentialId = 12345
  - `6602.mp4` → sequentialId = 6602
- **Mapping**: 1 file → 1 observation

#### Pattern 2: Multiple media per observation
- **Format**: `{sequentialId}{letter}.{extension}` where letter is a-z (lowercase)
- **Examples**:
  - `6602.jpg` + `6602a.jpg` + `6602b.jpg` → all link to sequentialId = 6602
  - `1234.mp4` + `1234a.jpg` → both link to sequentialId = 1234
- **Mapping**: Multiple files → 1 observation (hasMany relationship)
- **Letter handling**: Base file has no letter, subsequent files use a, b, c, etc.

#### Pattern 3: Shared media across observations (rare)
- **Format**: `{sequentialId1}-{sequentialId2}.{extension}` (range notation)
- **Examples**:
  - `6000-6001.jpg` → creates Media records for BOTH observations 6000 and 6001
  - `1200-1205.mp4` → creates Media records for observations 1200, 1201, 1202, 1203, 1204, 1205
- **Mapping**: 1 file → Multiple observations (same S3 URL reused)
- **S3 storage**: File stored once using first sequentialId in key

#### Invalid Patterns (mark as "needs_review" status)
Files that don't match any valid pattern:
- **Non-numeric base**: `abc123.jpg`, `photo.jpg`
- **Invalid suffixes**: `6602x.jpg` (must be lowercase a-z only)
- **Missing extension**: `6602`
- **Unsupported extension**: `6602.pdf`, `6602.txt`
- **Multiple hyphens**: `6000-6001-6002.jpg` (only single range supported)

**Valid extensions**: `jpg`, `jpeg`, `png`, `gif`, `mp4`, `mov`, `avi` (case-insensitive)

**Regex patterns**:
- Single: `^\d+\.(jpg|jpeg|png|gif|mp4|mov|avi)$`
- Multiple: `^\d+[a-z]\.(jpg|jpeg|png|gif|mp4|mov|avi)$`
- Range: `^\d+-\d+\.(jpg|jpeg|png|gif|mp4|mov|avi)$`

### MECO Schema

#### Observation Model
```typescript
sequentialId: a.integer().required(),  // Indexed - use for lookup
media: a.hasMany("Media", "observationId"),
```

#### Media Model
```typescript
url: a.url().required(),              // S3 URL after upload
type: a.enum(['IMAGE', 'VIDEO']),     // Derived from file extension
observationId: a.id().required(),     // FK to Observation.id
isAvailableForPublicUse: a.boolean().required(),  // Default: false
```

### S3 Key Format
```
media/{first_observation_id}/{original_filename}

Examples:

1. Single media:
   Input: 12345.jpg
   Query: sequentialId=12345 → observation.id = "abc-123"
   S3 key: media/abc-123/12345.jpg
   S3 URL: https://bucket.s3.region.amazonaws.com/media/abc-123/12345.jpg
   Media records: 1 (for observation abc-123)

2. Multiple media (same observation):
   Input: 6602.jpg, 6602a.jpg, 6602b.jpg
   Query: sequentialId=6602 → observation.id = "def-456"
   S3 keys:
     - media/def-456/6602.jpg
     - media/def-456/6602a.jpg
     - media/def-456/6602b.jpg
   Media records: 3 (all for observation def-456, different S3 URLs)

3. Range media (shared across observations):
   Input: 6000-6001.jpg
   Query 1: sequentialId=6000 → observation.id = "ghi-789"
   Query 2: sequentialId=6001 → observation.id = "jkl-012"
   S3 key: media/ghi-789/6000-6001.jpg (stored once, using first observation.id)
   S3 URL: https://bucket.s3.region.amazonaws.com/media/ghi-789/6000-6001.jpg
   Media records: 2 (both point to same S3 URL)
     - Media for observation ghi-789 (sequentialId=6000)
     - Media for observation jkl-012 (sequentialId=6001)
```

---

## Configuration

### Config File Location
`~/.amplify-media-migrator/config.json`

### Config Structure
```json
{
  "google_drive": {
    "folder_id": "1ABC...",
    "credentials_path": "~/.amplify-media-migrator/google_credentials.json",
    "token_path": "~/.amplify-media-migrator/google_token.json"
  },
  "aws": {
    "region": "us-east-1",
    "cognito": {
      "user_pool_id": "us-east-1_xxxxx",
      "client_id": "xxxxxxxxx",
      "username": "admin@example.com"
    },
    "amplify": {
      "api_endpoint": "https://xxx.appsync-api.region.amazonaws.com/graphql",
      "storage_bucket": "amplify-xxx-media"
    }
  },
  "migration": {
    "concurrency": 10,
    "retry_attempts": 3,
    "retry_delay_seconds": 5,
    "chunk_size_mb": 8,
    "default_media_public": false
  }
}
```

### Progress File
`~/.amplify-media-migrator/progress_{folder_id}.json`

```json
{
  "folder_id": "1ABC...",
  "started_at": "2026-01-25T10:00:00Z",
  "updated_at": "2026-01-25T12:30:00Z",
  "total_files": 20000,
  "files": {
    "file_id_123": {
      "filename": "12345.jpg",
      "status": "completed",
      "sequential_id": 12345,
      "observation_id": "abc-123",
      "s3_url": "https://...",
      "media_id": "media-456",
      "error": null
    },
    "file_id_456": {
      "filename": "invalid.txt",
      "status": "orphan",
      "error": "No sequentialId extracted"
    },
    "file_id_789": {
      "filename": "99999.jpg",
      "status": "failed",
      "sequential_id": 99999,
      "error": "Observation not found"
    }
  },
  "summary": {
    "pending": 18500,
    "completed": 1450,
    "failed": 3,
    "orphan": 47
  }
}
```

**Status states**:
- `pending` - Not yet processed
- `downloaded` - Downloaded from Google Drive (temp state)
- `uploaded` - Uploaded to S3 (temp state)
- `completed` - Media record created successfully
- `failed` - Retryable error (will retry on resume)
- `orphan` - No matching Observation (skip, don't retry)
- `needs_review` - Invalid filename pattern (user must rename and retry)
- `partial` - Some observations succeeded, some failed (for range files like 6000-6001.jpg)

---

## GraphQL Operations

### Query Observation by sequentialId
```graphql
query GetObservationBySequentialId($sequentialId: Int!) {
  listObservations(filter: { sequentialId: { eq: $sequentialId } }) {
    items {
      id
      sequentialId
    }
  }
}
```

**Expected response**:
- Success: Returns single observation with `id`
- Not found: Returns empty `items` array
- Handle as orphan if not found

### Create Media Record
```graphql
mutation CreateMedia($input: CreateMediaInput!) {
  createMedia(input: $input) {
    id
    url
    observationId
    type
    isAvailableForPublicUse
  }
}
```

**Input**:
```json
{
  "url": "https://bucket.s3.region.amazonaws.com/media/obs-id/12345.jpg",
  "observationId": "abc-123-def",
  "type": "IMAGE",
  "isAvailableForPublicUse": false
}
```

### Check for Existing Media (optional, prevents duplicates)
```graphql
query GetMediaByUrl($url: AWSUrl!) {
  listMedia(filter: { url: { eq: $url } }) {
    items {
      id
    }
  }
}
```

---

## Error Handling

### Retryable Errors (retry up to 3 times)
- Network timeouts
- Rate limit errors (429) → exponential backoff with jitter
- S3 upload failures → retry with fresh download
- Temporary API errors (500, 502, 503)

### Non-Retryable Errors (mark failed, continue)
- Authentication failures → fail fast, exit program
- File not found in Google Drive → mark orphan
- Observation not found → mark orphan
- Invalid file format → mark orphan
- GraphQL schema validation errors → mark failed

### Error Logging
All errors logged to:
- Console (with progress bar)
- `~/.amplify-media-migrator/logs/migration_{timestamp}.log`
- Failed files exported to `~/.amplify-media-migrator/failed_{timestamp}.json` on completion

---

## Performance Constraints

### Google Drive API Rate Limits
- **User quota**: 1,000 queries per 100 seconds (10 QPS sustained)
- **Impact**: Even with 100 workers, you're limited to ~10 downloads/sec
- **Mitigation**: Built-in rate limiter, exponential backoff on 429 errors

### AWS S3 Limits
- **PUT requests**: 3,500 requests/sec per prefix (NOT a bottleneck)
- **Transfer acceleration**: Optional (costs extra, provides 2-5x speedup for international transfers)

### Recommended Settings
- **Concurrency**: 10 workers (balances throughput vs rate limits)
- **Chunk size**: 8 MB for multipart uploads
- **Timeout**: 300 seconds per file (handle large videos)

---

## CLI Commands

### Setup & Configuration
```bash
# Initial setup (interactive prompts)
amplify-media-migrator config

# Show current configuration
amplify-media-migrator show
```

### Migration Workflow
```bash
# Step 1: Scan folder (dry-run, validates mapping)
amplify-media-migrator scan --folder-id FOLDER_ID

# Step 2: Review files that need attention
amplify-media-migrator review
# Shows all files with "needs_review" status and their invalid patterns
# User can then rename files in Google Drive and re-run migration

# Step 3: Run migration
amplify-media-migrator migrate --folder-id FOLDER_ID

# With options
amplify-media-migrator migrate \
  --folder-id FOLDER_ID \
  --concurrency 10 \
  --dry-run \
  --skip-existing

# Step 4: Resume interrupted migration
amplify-media-migrator resume

# Step 5: Export results
amplify-media-migrator export --status needs_review --output review_files.json
amplify-media-migrator export --status orphan --output orphan_files.json
```

### Options
- `--folder-id`: Google Drive folder ID (required)
- `--concurrency`: Number of parallel workers (default: 10)
- `--dry-run`: Validate without uploading (default: false)
- `--skip-existing`: Skip files with existing Media records (default: false)
- `--verbose`: Enable debug logging (default: false)

---

## Project Structure

```
amplify-media-migrator/
├── amplify_media_migrator/
│   ├── __init__.py
│   ├── cli.py                    # Click CLI entry point
│   ├── config.py                 # Config management
│   │
│   ├── auth/
│   │   ├── __init__.py
│   │   ├── cognito.py            # AWS Cognito authentication
│   │   └── google_drive.py       # Google OAuth2 flow
│   │
│   ├── sources/
│   │   ├── __init__.py
│   │   └── google_drive.py       # Google Drive API client
│   │
│   ├── targets/
│   │   ├── __init__.py
│   │   ├── amplify_storage.py    # S3 upload (boto3)
│   │   └── graphql_client.py     # GraphQL queries/mutations
│   │
│   ├── migration/
│   │   ├── __init__.py
│   │   ├── engine.py             # Main orchestration
│   │   ├── progress.py           # Progress tracking
│   │   └── mapper.py             # Filename → sequentialId extraction
│   │
│   └── utils/
│       ├── __init__.py
│       ├── media.py              # File type detection
│       └── rate_limiter.py       # Google Drive rate limiting
│
├── tests/
│   ├── test_mapper.py
│   ├── test_engine.py
│   └── fixtures/
│
├── .gitignore
├── CLAUDE.md
├── README.md
├── requirements.txt
├── setup.py
└── pytest.ini
```

---

## Development Setup

### Prerequisites
- Python 3.9+
- Google Cloud project with Drive API enabled
- AWS account with Amplify backend deployed
- Cognito user in ADMINS group

### Installation
```bash
# Clone repo
git clone <repo-url>
cd amplify-media-migrator

# Create virtual environment
python -m venv venv
source venv/bin/activate  # or `venv\Scripts\activate` on Windows

# Install in editable mode with dev dependencies
pip install -e ".[dev]"
```

### Dependencies (requirements.txt)
```
# Core
click>=8.1.0
boto3>=1.34.0
google-api-python-client>=2.0.0
google-auth-httplib2>=0.2.0
google-auth-oauthlib>=1.0.0
gql[requests]>=3.5.0
requests>=2.31.0
python-dateutil>=2.8.0

# UI
rich>=13.7.0
tqdm>=4.66.0

# Dev dependencies
pytest>=7.4.0
pytest-cov>=4.1.0
pytest-asyncio>=0.21.0
mypy>=1.8.0
black>=24.1.0
```

### Google Drive Setup
User must create OAuth2 credentials:
1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create project → Enable Drive API
3. Create OAuth2 credentials (Desktop app type)
4. Download `credentials.json` → place in `~/.amplify-media-migrator/google_credentials.json`

### Testing
```bash
# Run all tests
pytest

# With coverage
pytest --cov=amplify_media_migrator

# Specific test file
pytest tests/test_mapper.py -v
```

### Type Checking
```bash
mypy amplify_media_migrator
```

### Code Formatting
```bash
black amplify_media_migrator tests
```

---

## Code Style & Patterns

### Type Hints
All functions must have type hints:
```python
from typing import Optional, List, Dict
from pathlib import Path

def extract_sequential_id(filename: str) -> Optional[int]:
    """Extract sequentialId from filename."""
    ...
```

### Async/Await
Use async for I/O operations:
```python
async def download_file(file_id: str) -> bytes:
    """Download file from Google Drive."""
    ...

async def migrate_files(files: List[str]) -> None:
    """Migrate files with concurrency."""
    tasks = [migrate_single_file(f) for f in files]
    await asyncio.gather(*tasks)
```

### Error Handling Pattern
```python
from typing import Tuple

async def process_file(file_id: str) -> Tuple[bool, Optional[str]]:
    """
    Process a single file.
    
    Returns:
        (success, error_message)
    """
    try:
        # ... processing logic
        return True, None
    except RetryableError as e:
        logger.warning(f"Retryable error: {e}")
        return False, str(e)
    except NonRetryableError as e:
        logger.error(f"Non-retryable error: {e}")
        return False, str(e)
```

### Logging
Use structured logging:
```python
import logging

logger = logging.getLogger(__name__)

logger.info("Starting migration", extra={
    "folder_id": folder_id,
    "total_files": total_files,
})

logger.error("Failed to process file", extra={
    "file_id": file_id,
    "error": str(e),
}, exc_info=True)
```

---

## Testing Strategy

### Unit Tests
- Test filename extraction logic (mapper.py)
- Test media type detection (utils/media.py)
- Test config validation (config.py)

### Integration Tests (mocked)
- Mock Google Drive API responses
- Mock S3 uploads (use moto library)
- Mock GraphQL responses

### Fixtures
Store sample data in `tests/fixtures/`:
- `sample_google_drive_files.json`
- `sample_observation_responses.json`
- `sample_progress_file.json`

---

## Workflow Guidelines

### Before Starting Implementation
1. Read relevant files to understand existing patterns
2. Create a plan with approach and edge cases
3. Identify files that need modification

### During Implementation
- Write self-documenting code (minimize comments)
- Add type hints to all functions
- Handle errors explicitly (don't use bare `except`)
- Log important state transitions

### After Implementation
- Run type checker: `mypy amplify_media_migrator`
- Run tests: `pytest`
- Format code: `black amplify_media_migrator`
- Update README if needed

### Comments & Documentation
- Only add comments to explain **why**, not **what**
- Use docstrings for public functions
- Keep docstrings concise (1-2 sentences)

---

## Versioning

Follow semantic versioning:
- **Patch** (0.0.x): Bug fixes
- **Minor** (0.x.0): New features, backward compatible
- **Major** (x.0.0): Breaking changes

---

## Environment Variables

Optional overrides for configuration:
```bash
export AWS_REGION=us-east-1
export AMPLIFY_API_ENDPOINT=https://xxx.appsync-api.region.amazonaws.com/graphql
export GOOGLE_APPLICATION_CREDENTIALS=~/.amplify-media-migrator/google_credentials.json
export LOG_LEVEL=DEBUG
```

---

## Common Issues & Troubleshooting

| Issue | Solution |
|-------|----------|
| "Token expired" | Delete `google_token.json`, re-authenticate |
| "Access denied to Drive folder" | Ensure folder is shared with OAuth account |
| "User not in ADMINS group" | Add user to ADMINS in Cognito console |
| "S3 bucket not found" | Deploy Amplify storage first (`npx ampx sandbox`) |
| "Observation not found" | Verify sequentialId exists, check filename pattern |
| "Rate limit exceeded" | Reduce `--concurrency` value |

---

## Decision-Making Principles

- Never make claims about code behavior without investigating
- When uncertain, explore the code first, then plan
- Prefer simple solutions over complex ones
- Optimize for readability and maintainability
- Handle edge cases explicitly (don't assume happy path)

---

## Next Steps

1. Initialize project structure
2. Implement authentication modules (Cognito + Google Drive)
3. Implement Google Drive source client
4. Implement S3 + GraphQL target clients
5. Implement migration engine with progress tracking
6. Add comprehensive tests
7. Create user-facing README with setup instructions
