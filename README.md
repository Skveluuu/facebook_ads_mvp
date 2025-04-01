# Facebook Ads API MVP

This project demonstrates integration with the Facebook Marketing API using Python, focusing on common advertising operations like managing campaigns, ad sets, and ads.

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Create a `.env` file in the project root with your Facebook API credentials:
```bash
FB_APP_ID=your_app_id
FB_APP_SECRET=your_app_secret
FB_ACCESS_TOKEN=your_access_token
FB_AD_ACCOUNT_ID=act_your_ad_account_id
```

3. Ensure your access token has the following permissions:
- ads_management
- ads_read

## Project Structure

```
├── requirements.txt         # Project dependencies
├── .env                    # Environment variables (not in git)
├── src/
│   ├── config.py          # Configuration and environment setup
│   ├── fb_client.py       # Facebook API client wrapper
│   └── examples/          # Example usage scripts
│       ├── campaigns.py   # Campaign management examples
│       └── ads.py        # Ad management examples
```

## Usage Examples

1. List all campaigns:
```python
from src.examples.campaigns import list_campaigns
list_campaigns()
```

2. Create a new campaign:
```python
from src.examples.campaigns import create_campaign
create_campaign("My Test Campaign", "LINK_CLICKS")
```

3. Manage ads:
```python
from src.examples.ads import pause_ad
pause_ad("ad_id_here")
```

## Error Handling

The examples include basic error handling for common Facebook API errors:
- Invalid/expired access token
- Permission issues
- Rate limiting
- Invalid parameters

## Best Practices

1. Always use environment variables for sensitive credentials
2. Monitor access token expiration
3. Implement proper error handling
4. Use the Facebook Business SDK's built-in pagination handling

## Contributing

Feel free to submit issues and enhancement requests!
