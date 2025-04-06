from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.ad import Ad
from facebook_business.api import FacebookAdsApi
from typing import List, Dict, Optional
import json
import logging
import os

from .utils.date_utils import get_date_range, date_range_iterator, format_date

logger = logging.getLogger(__name__)

class FacebookAPI:
    def __init__(self, access_token: str, ad_account_id: str, api_version: str = 'v19.0'):
        """Initialize Facebook API client."""
        self.ad_account_id = ad_account_id if ad_account_id.startswith('act_') else f'act_{ad_account_id}'
        FacebookAdsApi.init(access_token=access_token, api_version=api_version)
        self.account = AdAccount(self.ad_account_id)
    
    def get_ads(self) -> List[Dict]:
        """Fetch ads with detailed information including budget, performance metrics, and targeting."""
        try:
            # Basic ad fields
            fields = [
                'id',
                'name',
                'status',
                'campaign_id',
                'adset_id',
                'created_time',
                'updated_time',
                'effective_status',
                
                # Budget fields (from adset)
                'adset.daily_budget',
                'adset.lifetime_budget',
                
                # Performance metrics
                'insights.fields(impressions,clicks,ctr,reach,frequency,spend)',
                
                # Conversion metrics
                'insights.fields(actions,action_values,cost_per_action_type,conversion_values)',
                'tracking_specs',
                'conversion_specs',
                'pixel_id',
                'custom_conversion_ids',
                
                # Targeting information
                'targeting',
                'placement',
                'optimization_goal',
                
                # Scheduling
                'adset.start_time',
                'adset.end_time',
                
                # Review and delivery
                'review_feedback',
                'delivery_info',
                
                # Creative details
                'creative'
            ]
            
            # Get the ads
            ads = AdAccount(self.ad_account_id).get_ads(
                fields=fields,
                params={
                    'date_preset': 'last_30d',  # Get data for last 30 days
                }
            )
            
            processed_ads = []
            for ad in ads:
                try:
                    ad_dict = ad.export_all_data()
                    
                    # Initialize default values
                    ad_dict.update({
                        'impressions': 0,
                        'clicks': 0,
                        'ctr': 0,
                        'reach': 0,
                        'frequency': 0,
                        'daily_budget': None,
                        'lifetime_budget': None,
                        'start_time': None,
                        'end_time': None,
                        'budget_remaining': None,
                        'targeting': None,
                        'delivery_info': None,
                        'detailed_creatives': [],
                        
                        # Initialize conversion metrics
                        'total_conversions': 0,
                        'cost_per_conversion': 0,
                        'conversion_rate': 0,
                        'conversion_value': 0,
                        'website_purchases': 0,
                        'website_adds_to_cart': 0,
                        'website_checkouts_initiated': 0,
                        'website_leads': 0,
                        'website_registrations': 0,
                        'website_content_views': 0,
                        'offsite_conversion_value': 0
                    })
                    
                    # Process insights data if available
                    if isinstance(ad_dict.get('insights'), dict) and isinstance(ad_dict['insights'].get('data'), list) and ad_dict['insights']['data']:
                        insights = ad_dict['insights']['data'][0]
                        
                        # Basic metrics
                        ad_dict.update({
                            'impressions': int(insights.get('impressions', 0)),
                            'clicks': int(insights.get('clicks', 0)),
                            'ctr': float(insights.get('ctr', 0)),
                            'reach': int(insights.get('reach', 0)),
                            'frequency': float(insights.get('frequency', 0))
                        })
                        
                        # Process conversion data
                        actions = insights.get('actions', [])
                        action_values = insights.get('action_values', [])
                        cost_per_action = insights.get('cost_per_action_type', [])
                        
                        # Process website actions
                        for action in actions:
                            action_type = action.get('action_type', '')
                            action_value = int(action.get('value', 0))
                            
                            if action_type == 'purchase':
                                ad_dict['website_purchases'] = action_value
                            elif action_type == 'add_to_cart':
                                ad_dict['website_adds_to_cart'] = action_value
                            elif action_type == 'initiate_checkout':
                                ad_dict['website_checkouts_initiated'] = action_value
                            elif action_type == 'lead':
                                ad_dict['website_leads'] = action_value
                            elif action_type == 'complete_registration':
                                ad_dict['website_registrations'] = action_value
                            elif action_type == 'view_content':
                                ad_dict['website_content_views'] = action_value
                        
                        # Calculate total conversions and values
                        total_conversions = sum(int(action.get('value', 0)) for action in actions)
                        total_value = sum(float(value.get('value', 0)) for value in action_values)
                        spend = float(insights.get('spend', 0))
                        
                        ad_dict.update({
                            'total_conversions': total_conversions,
                            'conversion_value': total_value,
                            'offsite_conversion_value': total_value,
                            'cost_per_conversion': (spend / total_conversions) if total_conversions > 0 else 0,
                            'conversion_rate': (total_conversions / int(insights.get('impressions', 1))) if int(insights.get('impressions', 0)) > 0 else 0
                        })
                        
                        # Store spend for budget calculation
                        spend = float(insights.get('spend', 0))
                    else:
                        spend = 0
                    
                    # Process adset data
                    adset_data = ad_dict.get('adset', {})
                    if isinstance(adset_data, dict):
                        ad_dict.update({
                            'daily_budget': adset_data.get('daily_budget'),
                            'lifetime_budget': adset_data.get('lifetime_budget'),
                            'start_time': adset_data.get('start_time'),
                            'end_time': adset_data.get('end_time')
                        })
                    
                    # Calculate budget remaining (if available)
                    if ad_dict.get('daily_budget'):
                        ad_dict['budget_remaining'] = float(ad_dict['daily_budget']) - spend
                    elif ad_dict.get('lifetime_budget'):
                        ad_dict['budget_remaining'] = float(ad_dict['lifetime_budget']) - spend
                    
                    # Process targeting into JSON
                    targeting_data = ad_dict.get('targeting')
                    if isinstance(targeting_data, (dict, list)):
                        ad_dict['targeting'] = json.dumps(targeting_data)
                    
                    # Process delivery info into JSON
                    delivery_info_data = ad_dict.get('delivery_info')
                    if isinstance(delivery_info_data, (dict, list)):
                        ad_dict['delivery_info'] = json.dumps(delivery_info_data)
                    
                    # Get creative details
                    creative_data = ad_dict.get('creative', {})
                    if isinstance(creative_data, dict):
                        creative_dict = {
                            'title': creative_data.get('title'),
                            'body': creative_data.get('body'),
                            'image_url': creative_data.get('image_url'),
                            'video_url': creative_data.get('video_url'),
                            'call_to_action': (creative_data.get('call_to_action') or {}).get('type'),
                            'link_url': creative_data.get('link_url')
                        }
                        ad_dict['detailed_creatives'] = [creative_dict]
                    
                    # Store pixel and custom conversion IDs
                    ad_dict['pixel_id'] = ad_dict.get('pixel_id')
                    if isinstance(ad_dict.get('custom_conversion_ids'), (list, dict)):
                        ad_dict['custom_conversion_ids'] = json.dumps(ad_dict['custom_conversion_ids'])
                    
                    processed_ads.append(ad_dict)
                    
                except Exception as ad_error:
                    print(f"Error processing ad {ad.get('id', 'unknown')}: {str(ad_error)}")
                    continue
            
            return processed_ads
            
        except Exception as e:
            print(f"Error fetching ads: {str(e)}")
            return []

    def fetch_offsite_conversions(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        days_back: int = 30,
        increment_days: int = 1,
        output_file: str = 'data/output/offsite_conversions.json'
    ) -> List[Dict]:
        """
        Fetch offsite conversion data within date range, day by day.
        
        Args:
            start_date: Optional start date (YYYY-MM-DD)
            end_date: Optional end date (YYYY-MM-DD)
            days_back: Days to look back if no start_date
            increment_days: Days per fetch (default: 1 for daily)
            output_file: Path to save JSON output
        """
        start_dt, end_dt = get_date_range(start_date, end_date, days_back)
        logger.info(f"Fetching data from {format_date(start_dt)} to {format_date(end_dt)}")
        
        all_data = []
        for period_start, period_end in date_range_iterator(start_dt, end_dt, increment_days):
            logger.info(f"Fetching period {format_date(period_start)} to {format_date(period_end)}")
            
            params = {
                'time_range': {
                    'since': format_date(period_start),
                    'until': format_date(period_end)
                },
                'level': 'ad',
                'filtering': [],
                'breakdowns': [],
                'fields': [
                    'ad_id',
                    'ad_name',
                    'campaign_id',
                    'campaign_name',
                    'impressions',
                    'clicks',
                    'spend',
                    'actions',
                    'action_values'
                ]
            }
            
            try:
                insights = self.account.get_insights(params=params)
                period_data = [insight.export_all_data() for insight in insights]
                all_data.extend(period_data)
                logger.info(f"Retrieved {len(period_data)} records for period")
            except Exception as e:
                logger.error(f"Error fetching data for period {format_date(period_start)}: {str(e)}")
                continue
        
        # Save to JSON file
        if output_file:
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            with open(output_file, 'w') as f:
                json.dump(all_data, f, indent=2)
            logger.info(f"Data saved to {output_file}")
        
        return all_data

    def fetch_ad_creatives(
        self,
        ad_ids: List[str],
        output_file: str = 'data/output/ad_creatives.json'
    ) -> List[Dict]:
        """Fetch creative details for specified ads."""
        logger.info(f"Fetching creative details for {len(ad_ids)} ads")
        
        fields = [
            'id',
            'title',
            'body',
            'image_url',
            'thumbnail_url',
            'object_story_spec',
            'call_to_action_type'
        ]
        
        creatives = []
        for ad_id in ad_ids:
            try:
                ad = Ad(ad_id)
                creative = ad.get_ad_creatives(fields=fields)
                if creative:
                    creatives.extend([c.export_all_data() for c in creative])
            except Exception as e:
                logger.error(f"Error fetching creative for ad {ad_id}: {str(e)}")
                continue
        
        logger.info(f"Successfully fetched {len(creatives)} creatives")
        
        # Save to JSON file
        if output_file:
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            with open(output_file, 'w') as f:
                json.dump(creatives, f, indent=2)
            logger.info(f"Creative data saved to {output_file}")
        
        return creatives 