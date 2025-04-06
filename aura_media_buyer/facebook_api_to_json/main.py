import json
import logging
import os
from datetime import datetime, timedelta
from typing import List, Dict
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adcreative import AdCreative
from facebook_business.adobjects.adsinsights import AdsInsights
from dotenv import load_dotenv
from src.utils.date_utils import get_date_range, date_range_iterator, format_date

# Load environment variables and configure logging
load_dotenv()
logging.basicConfig(
    level=os.getenv('LOG_LEVEL', 'INFO'),
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Constants
OUTPUT_DIR = os.path.join('facebook_api_to_json', 'data', 'output')

def init_facebook_api():
    """Initialize Facebook Ads API with credentials from environment variables."""
    try:
        access_token = os.getenv('FACEBOOK_ACCESS_TOKEN')
        app_id = os.getenv('FACEBOOK_APP_ID')
        app_secret = os.getenv('FACEBOOK_APP_SECRET')
        account_id = os.getenv('FACEBOOK_AD_ACCOUNT_ID')

        if not all([access_token, app_id, app_secret, account_id]):
            raise ValueError("Missing required Facebook API credentials")

        # Remove 'act_' prefix if it exists, as it will be added by the API
        account_id = account_id.replace('act_', '')

        FacebookAdsApi.init(app_id, app_secret, access_token)
        return account_id

    except Exception as e:
        logging.error(f"Error initializing Facebook API: {e}")
        raise

def format_date(date):
    """Format datetime object to YYYY-MM-DD string."""
    return date.strftime('%Y-%m-%d')

def get_correct_conversion_count(insight_data):
    """
    Get the correct conversion count from actions and action_values arrays.
    Returns the smaller value if it's approximately 1/35th of the larger value.
    """
    actions = insight_data.get('actions', [])
    action_values = insight_data.get('action_values', [])
    
    conversion_count = None
    conversion_value = None
    
    for action in actions:
        if action.get('action_type') == 'offsite_conversion.fb_pixel_custom':
            try:
                conversion_count = int(float(action.get('1d_click', 0)))
            except (ValueError, TypeError):
                continue
    
    for value in action_values:
        if value.get('action_type') == 'offsite_conversion.fb_pixel_custom':
            try:
                conversion_value = int(float(value.get('1d_click', 0)))
            except (ValueError, TypeError):
                continue
    
    # If either value is None, we can't proceed
    if conversion_count is None or conversion_value is None:
        return None
    
    # If both values are 0, return 0
    if conversion_count == 0 and conversion_value == 0:
        return 0
        
    # If one value is 0 but not the other, something is wrong
    if conversion_count == 0 or conversion_value == 0:
        return None
    
    # The smaller value should be approximately 1/35th of the larger value
    smaller = min(conversion_count, conversion_value)
    larger = max(conversion_count, conversion_value)
    
    # Check if the relationship is approximately 1:35
    if abs(larger / smaller - 35) < 1:  # Allow for small rounding differences
        return smaller
    
    return None

def fetch_ad_creatives(ad_ids):
    """Fetch creative details for the given ad IDs."""
    try:
        if not ad_ids:
            return []
        
        account_id = os.getenv('FACEBOOK_AD_ACCOUNT_ID')
        if not account_id:
            raise ValueError("FACEBOOK_AD_ACCOUNT_ID not found in environment variables")
        
        account_id = f'act_{account_id}'
        ad_account = AdAccount(account_id)
        
        # Get creative details for each ad
        creatives = []
        for ad_id in ad_ids:
            try:
                # Create Ad object and get its creative
                ad = Ad(ad_id)
                ad_creatives = ad.get_ad_creatives(fields=[
                    AdCreative.Field.id,
                    AdCreative.Field.name,
                    AdCreative.Field.title,
                    AdCreative.Field.body,
                    AdCreative.Field.image_url,
                    AdCreative.Field.video_id,
                    AdCreative.Field.object_story_spec,
                    AdCreative.Field.url_tags
                ])
                
                if ad_creatives:
                    creatives.append(ad_creatives[0].export_all_data())
            except Exception as e:
                logging.error(f"Error fetching creative for ad {ad_id}: {str(e)}")
                continue
        
        # Save creatives to file
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        creatives_file = os.path.join(OUTPUT_DIR, 'ad_creatives.json')
        with open(creatives_file, 'w') as f:
            json.dump(creatives, f, indent=2)
        
        logging.info(f"Fetched creative details for {len(creatives)} ads")
        return creatives
    
    except Exception as e:
        logging.error(f"Error fetching ad creatives: {str(e)}")
        return []

def process_insights_data(insights_data):
    processed_data = []
    for insight_data in insights_data:
        logging.info(f"Processing insight for ad: {insight_data.get('ad_name')}")
        
        if 'actions' in insight_data:
            logging.info("Raw actions:")
            for action in insight_data['actions']:
                logging.info(f"  {json.dumps(action)}")
                
        if 'action_values' in insight_data:
            logging.info("Raw action_values:")
            for value in insight_data['action_values']:
                logging.info(f"  {json.dumps(value)}")

        # Get correct conversion count
        correct_count = get_correct_conversion_count(insight_data)
        if correct_count is not None:
            filtered_items = [{
                'action_type': 'offsite_conversion.fb_pixel_custom',
                '1d_click': str(correct_count)
            }]
            insight_data['actions'] = filtered_items
            insight_data.pop('action_values', None)  # Remove redundant data
        else:
            # If no valid conversion count found, remove both arrays
            insight_data.pop('actions', None)
            insight_data.pop('action_values', None)
            
        processed_data.append(insight_data)
    
    return processed_data

def fetch_offsite_conversions():
    """Fetch offsite conversion data from Facebook Ads API."""
    try:
        account_id = init_facebook_api()
        ad_account = AdAccount(f'act_{account_id}')
        
        # Define fields to retrieve
        fields = [
            AdsInsights.Field.ad_id,
            AdsInsights.Field.ad_name,
            AdsInsights.Field.adset_id,
            AdsInsights.Field.adset_name,
            AdsInsights.Field.campaign_id,
            AdsInsights.Field.campaign_name,
            AdsInsights.Field.impressions,
            AdsInsights.Field.clicks,
            AdsInsights.Field.spend,
            AdsInsights.Field.actions,
            AdsInsights.Field.action_values
        ]
        
        # Set date range
        end_date = datetime(2025, 4, 6)
        start_date = end_date - timedelta(days=30)
        
        all_data = []
        all_ad_ids = set()
        current_date = start_date
        
        while current_date < end_date:
            period_end = current_date + timedelta(days=1)
            
            logging.info(f"Fetching data for {format_date(current_date)}")
            
            params = {
                'time_range': {
                    'since': format_date(current_date),
                    'until': format_date(period_end)
                },
                'level': 'ad',
                'limit': 100,
                'action_attribution_windows': ['1d_click'],
                'action_report_time': 'conversion',
                'filtering': [{
                    'field': 'action_type',
                    'operator': 'IN',
                    'value': ['offsite_conversion.fb_pixel_custom']
                }]
            }
            
            insights = ad_account.get_insights(fields=fields, params=params)
            insights_list = list(insights)
            
            if insights_list:
                logging.info(f"Raw insights count: {len(insights_list)}")
                processed_data = []
                
                for insight in insights_list:
                    insight_data = insight.export_all_data()
                    insight_data['date_start'] = format_date(current_date)
                    insight_data['date_stop'] = format_date(period_end)
                    
                    # Get correct conversion count
                    correct_count = get_correct_conversion_count(insight_data)
                    if correct_count is not None:
                        filtered_items = [{
                            'action_type': 'offsite_conversion.fb_pixel_custom',
                            '1d_click': str(correct_count)
                        }]
                        insight_data['actions'] = filtered_items
                        insight_data.pop('action_values', None)  # Remove redundant data
                        processed_data.append(insight_data)
                        all_ad_ids.add(insight_data['ad_id'])
                
                all_data.extend(processed_data)
                logging.info(f"Retrieved {len(processed_data)} records for {format_date(current_date)}")
            
            current_date = period_end
        
        logging.info(f"Total records retrieved: {len(all_data)}")
        
        # Log a sample record
        if all_data:
            logging.info("Sample record before saving: " + json.dumps(all_data[0], indent=2))
        
        # Save insights data
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        insights_file = os.path.join(OUTPUT_DIR, 'offsite_conversions.json')
        with open(insights_file, 'w') as f:
            json.dump(all_data, f, indent=2)
        logging.info(f"Data saved to {insights_file}")
        
        # Fetch and save ad creatives
        creatives = fetch_ad_creatives(list(all_ad_ids))
        
        return all_data, creatives
    
    except Exception as e:
        logging.error(f"Error fetching offsite conversions: {str(e)}")
        raise

if __name__ == "__main__":
    fetch_offsite_conversions()