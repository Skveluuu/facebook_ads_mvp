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

# Load environment variables and configure logging
load_dotenv()
logging.basicConfig(
    level=os.getenv('LOG_LEVEL', 'INFO'),
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def init_facebook_api():
    """Initialize the Facebook API with credentials from environment variables."""
    app_id = os.getenv('FACEBOOK_APP_ID')
    app_secret = os.getenv('FACEBOOK_APP_SECRET')
    access_token = os.getenv('FACEBOOK_ACCESS_TOKEN')
    api_version = os.getenv('FACEBOOK_API_VERSION', 'v19.0')
    
    logging.info(f"Initializing Facebook API with version {api_version}")
    FacebookAdsApi.init(app_id, app_secret, access_token, api_version=api_version)

def fetch_ad_creatives(ad_ids: List[str]) -> List[Dict]:
    """Fetch creative details including images for multiple ads."""
    try:
        creatives = []
        fields = [
            'id',
            'title',
            'body',
            'image_url',
            'thumbnail_url',
            'object_story_spec',
            'call_to_action_type',
            'link_url'
        ]

        logging.info(f"Fetching creative details for {len(ad_ids)} ads")
        
        for ad_id in ad_ids:
            try:
                ad = Ad(ad_id)
                creative_id = ad.get_ad_creatives(fields=['id'])[0]['id']
                creative = AdCreative(creative_id)
                creative_data = creative.api_get(fields=fields)
                
                creatives.append(creative_data.export_all_data())
                logging.debug(f"Fetched creative for ad {ad_id}")
                
            except Exception as e:
                logging.error(f"Error fetching creative for ad {ad_id}: {str(e)}")
                continue
        
        logging.info(f"Successfully fetched {len(creatives)} creatives")
        
        # Save creatives to JSON file
        output_dir = "data/output"
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, "ad_creatives.json")
        with open(output_file, 'w') as f:
            json.dump(creatives, f, indent=2)
        logging.info(f"Creative data saved to {output_file}")
        
        return creatives
        
    except Exception as e:
        logging.error(f"Error in fetch_ad_creatives: {str(e)}")
        raise

def fetch_offsite_conversions():
    """Fetch offsite conversion data from Facebook Ads API."""
    try:
        init_facebook_api()
        account_id = os.getenv('FACEBOOK_AD_ACCOUNT_ID')
        if not account_id:
            raise ValueError("FACEBOOK_AD_ACCOUNT_ID not found in environment variables")
            
        logging.info(f"Fetching data for account ID: {account_id}")
        ad_account = AdAccount(account_id)
        
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
        
        # Set date range for last 30 days
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=30)
        
        params = {
            'time_range': {
                'since': start_date.strftime('%Y-%m-%d'),
                'until': end_date.strftime('%Y-%m-%d')
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
        
        logging.info(f"Fetching data from {start_date} to {end_date}")
        insights = ad_account.get_insights(fields=fields, params=params)
        
        # Process insights data
        data = []
        ad_ids = []  # Collect ad IDs for creative fetching
        for insight in insights:
            insight_data = insight.export_all_data()
            
            # Filter actions and action_values to keep only offsite_conversion.fb_pixel_custom with 1d_click
            for metric in ['actions', 'action_values']:
                if metric in insight_data:
                    filtered_items = [{
                        'action_type': item['action_type'],
                        '1d_click': item['1d_click']
                    } for item in insight_data[metric]
                        if item.get('action_type') == 'offsite_conversion.fb_pixel_custom'
                        and '1d_click' in item]
                    
                    if filtered_items:
                        insight_data[metric] = filtered_items
                    else:
                        insight_data.pop(metric, None)
            
            if 'actions' in insight_data:
                data.append(insight_data)
                ad_ids.append(insight_data['ad_id'])

        logging.info(f"Retrieved {len(data)} records")

        # Save to JSON file
        output_dir = "data/output"
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, "offsite_conversions.json")
        with open(output_file, 'w') as f:
            json.dump(data, f, indent=2)
        logging.info(f"Data saved to {output_file}")
        
        # Fetch and save ad creatives
        creatives = fetch_ad_creatives(ad_ids)
        
        return data, creatives

    except Exception as e:
        logging.error(f"Error fetching offsite conversions: {str(e)}")
        raise

if __name__ == "__main__":
    fetch_offsite_conversions()