# Facebook Ads API MVP

This project demonstrates integration with the Facebook Marketing API using Python, focusing on common advertising operations like managing campaigns, ad sets, and ads.

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/Skveluuu/facebook_ads_mvp.git
   cd facebook_ads_mvp
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Create a `.env` file in the project root with your Facebook API credentials:
   ```bash
   FB_APP_ID=your_app_id
   FB_APP_SECRET=your_app_secret
   FB_ACCESS_TOKEN=your_access_token
   FB_AD_ACCOUNT_ID=act_your_ad_account_id
   ```

4. Ensure your access token has the following permissions:
   - ads_management
   - ads_read

## Usage

1. List all campaigns:
   ```python
   from aura_media_buyer.facebook_api_to_json.src.examples.campaigns import list_campaigns
   list_campaigns()
   ```

2. Create a new campaign:
   ```python
   from aura_media_buyer.facebook_api_to_json.src.examples.campaigns import create_campaign
   create_campaign("My Test Campaign", "LINK_CLICKS")
   ```

3. Manage ads:
   ```python
   from aura_media_buyer.facebook_api_to_json.src.examples.ads import pause_ad
   pause_ad("ad_id_here")
   ```

## Contributing

Feel free to submit issues and enhancement requests!

## License

This project is licensed under the MIT License - see the LICENSE file for details. 