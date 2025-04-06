# Facebook Ads Data Fetcher

A Python script to fetch and analyze Facebook Ads data, specifically focusing on offsite conversions and ad creatives.

## Features

- Fetches offsite conversion data from Facebook Ads API
- Retrieves detailed ad creative information including images and CTAs
- Filters data for specific attribution windows (1d_click)
- Saves data in structured JSON format
- Comprehensive error handling and logging

## Prerequisites

- Python 3.7+
- Facebook Business Account
- Facebook App with necessary permissions
- Ad Account access

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd <repository-name>
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Create a `.env` file in the project root with your Facebook API credentials:
```
FACEBOOK_APP_ID=your_app_id
FACEBOOK_APP_SECRET=your_app_secret
FACEBOOK_ACCESS_TOKEN=your_access_token
FACEBOOK_AD_ACCOUNT_ID=act_your_ad_account_id
FACEBOOK_API_VERSION=v19.0
LOG_LEVEL=INFO
```

## Usage

Run the script:
```bash
python main.py
```

The script will:
1. Fetch offsite conversion data for the last 30 days
2. Retrieve creative details for all ads
3. Save data to JSON files in the `data/output` directory:
   - `offsite_conversions.json`: Contains conversion metrics and ad performance data
   - `ad_creatives.json`: Contains creative details including images and CTAs

## Output Structure

### Offsite Conversions Data
```json
{
  "ad_id": "...",
  "ad_name": "...",
  "campaign_id": "...",
  "campaign_name": "...",
  "impressions": "...",
  "clicks": "...",
  "spend": "...",
  "actions": [...],
  "action_values": [...]
}
```

### Ad Creatives Data
```json
{
  "id": "...",
  "title": "...",
  "body": "...",
  "image_url": "...",
  "thumbnail_url": "...",
  "object_story_spec": {...},
  "call_to_action_type": "..."
}
```

## Directory Structure

```
├── main.py              # Main script
├── requirements.txt     # Python dependencies
├── .env                # Environment variables (not in repo)
├── .gitignore         # Git ignore rules
├── README.md          # This file
└── data/
    └── output/        # JSON output files
```

## Error Handling

The script includes comprehensive error handling for:
- API authentication failures
- Network issues
- Missing or invalid data
- Rate limiting

All errors are logged with appropriate detail level.

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
