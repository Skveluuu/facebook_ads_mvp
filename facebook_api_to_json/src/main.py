import os
import json
from datetime import datetime, date, timedelta
import logging
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def init_facebook_api():
    """Initialize Facebook API with credentials from environment variables."""
    load_dotenv()
    app_id = os.getenv('FACEBOOK_APP_ID')
    app_secret = os.getenv('FACEBOOK_APP_SECRET')
    access_token = os.getenv('FACEBOOK_ACCESS_TOKEN')
    
    if not all([app_id, app_secret, access_token]):
        raise ValueError("Missing required Facebook API credentials in .env file")
    
    FacebookAdsApi.init(app_id, app_secret, access_token)
    return AdAccount(f"act_{os.getenv('FACEBOOK_AD_ACCOUNT_ID').replace('act_', '')}")

def fetch_daily_data(account, single_date):
    """Fetch data for a single day."""
    date_str = single_date.strftime('%Y-%m-%d')
    logging.info(f"Fetching data for {date_str}")
    
    try:
        # Fetch offsite conversions
        insights = account.get_insights(
            fields=[
                'account_id',
                'account_name',
                'campaign_id',
                'campaign_name',
                'adset_id',
                'adset_name',
                'ad_id',
                'ad_name',
                'impressions',
                'clicks',
                'spend',
                'actions',
                'action_values'
            ],
            params={
                'time_range': {'since': date_str, 'until': date_str},
                'action_attribution_windows': ['1d_click'],
                'level': 'ad'
            }
        )
        
        # Collect ad IDs for creative fetching
        ad_ids = [insight['ad_id'] for insight in insights]
        
        # Fetch ad creatives
        creatives = []
        for ad_id in ad_ids:
            creative = account.get_ads(
                fields=[
                    'id',
                    'creative',
                    'creative.fields(id,title,body,image_url,thumbnail_url,object_story_spec,call_to_action_type)'
                ],
                params={'ids': [ad_id]}
            )
            creatives.extend(creative)
        
        # Save data to date-specific files
        os.makedirs('data/output/daily', exist_ok=True)
        
        # Save conversions
        conversions_file = f'data/output/daily/conversions_{date_str}.json'
        with open(conversions_file, 'w') as f:
            json.dump([insight.export_all_data() for insight in insights], f, indent=2)
        logging.info(f"Saved {len(insights)} conversion records to {conversions_file}")
        
        # Save creatives
        creatives_file = f'data/output/daily/creatives_{date_str}.json'
        with open(creatives_file, 'w') as f:
            json.dump([creative.export_all_data() for creative in creatives], f, indent=2)
        logging.info(f"Saved {len(creatives)} creative records to {creatives_file}")
        
        return True
    
    except Exception as e:
        logging.error(f"Error fetching data for {date_str}: {str(e)}")
        return False

def main():
    """Main function to fetch data day by day."""
    try:
        account = init_facebook_api()
        
        # Default to last 30 days if not specified
        end_date = date.today()
        start_date = end_date - timedelta(days=30)
        
        logging.info(f"Fetching data from {start_date} to {end_date}")
        
        # Create output directory
        os.makedirs('data/output/daily', exist_ok=True)
        
        # Fetch data for each day
        current_date = start_date
        while current_date <= end_date:
            success = fetch_daily_data(account, current_date)
            if success:
                logging.info(f"Successfully processed data for {current_date}")
            else:
                logging.warning(f"Failed to process data for {current_date}")
            current_date += timedelta(days=1)
        
        logging.info("Data fetching completed")
        
    except Exception as e:
        logging.error(f"Error in main process: {str(e)}")
        raise

if __name__ == "__main__":
    main() 