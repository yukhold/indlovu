# Indlovu Analytics

Analytics toolkit for downloading App Store Connect reports and syncing them to Google Sheets.

## Features

- Download App Store Analytics reports (downloads, sessions, purchases, discovery)
- Automatic weekly sync via cron
- Upload reports to Google Sheets
- Generate markdown summaries with date ranges

## Installation

```bash
# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

## Configuration

### 1. App Store Connect API

1. Download your `.p8` key from [App Store Connect](https://appstoreconnect.apple.com) → Users and Access → Integrations → Keys
2. Create a `.env` file in the project root:

```ini
# App Store Connect API
ISSUER_ID=your-issuer-id
KEY_ID=your-key-id
PRIVATE_KEY_PATH=/path/to/AuthKey_XXXXX.p8
APP_ID=your-app-id

# Analytics request ID (get from: python cli.py --list-requests)
ANALYTICS_REQUEST_ID=your-request-id
```

### 2. Google Sheets (optional)

1. Create a project in [Google Cloud Console](https://console.cloud.google.com)
2. Enable Google Sheets API
3. Create a Service Account and download JSON key
4. Save it as `google_credentials.json` in the project root
5. Add to `.env`:

```ini
GOOGLE_CREDENTIALS_FILE=google_credentials.json
GOOGLE_SPREADSHEET_ID=your-spreadsheet-id
```

6. Share your Google Spreadsheet with the Service Account email

## Usage

### Manual Operations (CLI)

```bash
# List report requests
python cli.py --list-requests

# Create a new report request
python cli.py --create-request

# Download all reports
python cli.py --download-all --request-id <ID>

# List reports for a request
python cli.py --request-id <ID> --list-reports

# Download specific instance
python cli.py --instance-id <ID> --download
```

### Weekly Sync (Cron)

Run the full sync (download + Google Sheets upload):

```bash
python weekly_sync.py
```

Set up cron for automatic weekly sync (Sunday 21:00):

```bash
crontab -e
# Add:
0 21 * * 0 cd /path/to/Indlovu && .venv/bin/python weekly_sync.py >> logs/weekly_sync.log 2>&1
```

## Project Structure

```
Indlovu/
├── auth.py              # JWT token generation
├── appstore_api.py      # App Store Connect API client
├── google_sheets.py     # Google Sheets upload functions
├── cli.py               # Command-line interface
├── weekly_sync.py       # Weekly cron script
├── requirements.txt     # Python dependencies
├── .env                 # Environment variables (gitignored)
├── google_credentials.json  # Google API credentials (gitignored)
├── logs/                # Cron logs
└── reports/             # Downloaded CSV reports
    └── YYYY-MM-DD/      # Dated folders with reports
```

## Available Reports

| Report | Granularity | Description |
|--------|-------------|-------------|
| Downloads Standard | Daily | App downloads by source, device, territory |
| Downloads Detailed | Daily, Weekly, Monthly | Detailed download metrics |
| Purchases Standard | Daily, Weekly, Monthly | In-app purchases and subscriptions |
| Install/Delete | Daily, Weekly, Monthly | App installations and deletions |
| Sessions | Daily, Weekly, Monthly | App usage sessions |
| Discovery | Daily, Weekly, Monthly | App Store impressions and page views |

## Notes

- Reports are updated by Apple with a 2-3 day delay
- Historical data may be slightly adjusted for up to 7 days
- JWT tokens are valid for 20 minutes and auto-generated per request
