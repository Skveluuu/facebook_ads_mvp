"""
Data extractors for Facebook Ads data.
"""
from typing import Dict, List, Optional, Tuple, Union
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adcreative import AdCreative
from facebook_business.adobjects.customconversion import CustomConversion
from facebook_business.adobjects.customconversionstatsresult import CustomConversionStatsResult
from facebook_business.exceptions import FacebookRequestError
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adreportrun import AdReportRun
from facebook_business.adobjects.insightsresult import InsightsResult
import backoff
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import time
from facebook_business.adobjects.adspixel import AdsPixel
from facebook_business.adobjects.adspixelstatsresult import AdsPixelStatsResult
import json

class FacebookAdsExtractor:
    """Handles extraction of data from Facebook Ads API."""
    
    def __init__(self):
        """Initialize the Facebook API connection."""
        load_dotenv()
        self.api = self._init_api()
        self.account = self._get_ad_account()
        print("FacebookAdsExtractor initialized successfully")
    
    def _init_api(self) -> FacebookAdsApi:
        """Initialize Facebook API connection."""
        app_id = os.getenv('FACEBOOK_APP_ID')
        app_secret = os.getenv('FACEBOOK_APP_SECRET')
        access_token = os.getenv('FACEBOOK_ACCESS_TOKEN')
        
        if not all([app_id, app_secret, access_token]):
            raise ValueError("Missing required Facebook API credentials in .env file")
        
        print(f"Initializing Facebook API with app_id: {app_id}")
        return FacebookAdsApi.init(app_id, app_secret, access_token)
    
    def _get_ad_account(self) -> AdAccount:
        """Get Facebook Ad Account."""
        ad_account_id = os.getenv('FACEBOOK_AD_ACCOUNT_ID')
        if not ad_account_id:
            raise ValueError("Missing FACEBOOK_AD_ACCOUNT_ID in .env file")
        
        print(f"Using Ad Account ID: {ad_account_id}")
        return AdAccount(ad_account_id)
    
    @backoff.on_exception(backoff.expo, FacebookRequestError, max_tries=5)
    def extract_ads(self) -> List[Dict]:
        """Extract basic ad information."""
        print("Extracting ads from Facebook...")
        fields = [
            Ad.Field.id,
            Ad.Field.name,
            Ad.Field.status,
            Ad.Field.campaign_id,
            Ad.Field.adset_id,
            Ad.Field.created_time,
            Ad.Field.updated_time,
            Ad.Field.effective_status,
            Ad.Field.creative,
        ]
        
        try:
            print("Fetching ads with fields:", fields)
            ads = self.account.get_ads(fields=fields)
            ads_list = [ad.export_all_data() for ad in ads]
            print(f"Found {len(ads_list)} ads")
            return ads_list
        except FacebookRequestError as e:
            print(f"Facebook API Error: {str(e)}")
            print(f"Error Code: {e.api_error_code()}")
            print(f"Error Message: {e.api_error_message()}")
            print(f"Error Type: {e.api_error_type()}")
            raise
        except Exception as e:
            print(f"Unexpected error during ad extraction: {str(e)}")
            raise
    
    @backoff.on_exception(backoff.expo, FacebookRequestError, max_tries=5)
    def extract_creative(self, creative_id: str) -> Optional[Dict]:
        """Extract creative details for a given creative ID."""
        try:
            creative = AdCreative(creative_id)
            fields = [
                AdCreative.Field.id,
                AdCreative.Field.name,
                AdCreative.Field.title,
                AdCreative.Field.body,
                AdCreative.Field.image_url,
                AdCreative.Field.thumbnail_url,
                AdCreative.Field.object_story_spec,
                AdCreative.Field.url_tags,
                AdCreative.Field.link_url,
                AdCreative.Field.object_type,
                AdCreative.Field.template_url,
                AdCreative.Field.video_id,
                AdCreative.Field.call_to_action_type,
                AdCreative.Field.asset_feed_spec,
            ]
            return creative.api_get(fields=fields).export_all_data()
        except FacebookRequestError as e:
            print(f"Error extracting creative {creative_id}: {str(e)}")
            return None
    
    def extract_custom_conversion_data(self, conversion_name: str = None) -> Dict:
        """Extract data for a specific custom conversion."""
        try:
            # Get the pixel ID first
            pixel_id = self.get_pixel_id_from_account()
            if not pixel_id:
                print("No pixel found in account")
                return None

            # If no specific conversion name is provided, look for DTInstall
            if not conversion_name:
                conversion_name = 'dtinstall'

            # Normalize the conversion name for comparison
            conversion_name = conversion_name.lower().replace(' ', '')

            # Get custom conversions for the account
            custom_conversions = self.account.get_custom_conversions(fields=[
                'id',
                'name',
                'pixel_id',
                'creation_time',
                'last_fired_time',
                'custom_event_type',
                'default_conversion_value',
                'rule',
                'pixel_rule'
            ])

            # First, check if we already have a custom conversion for DTInstall
            for conversion in custom_conversions:
                conv_data = conversion.export_all_data()
                if conv_data.get('name', '').lower().replace(' ', '') == conversion_name:
                    print(f"Found existing custom conversion: {conv_data['name']}")
                    return {
                        'id': conv_data['id'],
                        'name': conv_data['name'],
                        'pixel_id': conv_data.get('pixel_id'),
                        'event_type': conv_data.get('custom_event_type'),
                        'creation_time': conv_data.get('creation_time'),
                        'last_fired_time': conv_data.get('last_fired_time'),
                        'default_value': conv_data.get('default_conversion_value', 0),
                        'rule': conv_data.get('rule') or conv_data.get('pixel_rule')
                    }

            # If we're specifically looking for DTInstall and it's not found as a custom conversion,
            # let's get it from pixel events
            if conversion_name == 'dtinstall':
                print("DTInstall custom conversion not found, checking pixel events...")
                pixel = AdsPixel(pixel_id)
                
                # Get pixel stats for DTInstall events
                stats = pixel.get_stats(fields=[
                    'event_name',
                    'value',
                    'count',
                    'timestamp'
                ], params={
                    'aggregation': 'event_name',
                    'event_name': ['DTInstall']
                })

                if stats:
                    print("Found DTInstall events in pixel")
                    # Create a custom conversion object for DTInstall
                    return {
                        'id': f'pixel_{pixel_id}_dtinstall',  # Create a unique ID
                        'name': 'DTInstall',
                        'pixel_id': pixel_id,
                        'event_type': 'custom_event',
                        'creation_time': None,  # We don't have this for pixel events
                        'last_fired_time': stats[0].get('timestamp') if stats else None,
                        'default_value': stats[0].get('value', 0) if stats else 0,
                        'rule': {
                            'event_name': 'DTInstall',
                            'pixel_id': pixel_id
                        }
                    }
                else:
                    print("No DTInstall events found in pixel")
                    return None

            print(f"Custom conversion '{conversion_name}' not found")
            return None

        except FacebookRequestError as e:
            print(f"Facebook API Error: {str(e)}")
            return None
        except Exception as e:
            print(f"Error extracting custom conversion data: {str(e)}")
            return None
    
    def list_custom_conversions(self) -> List[Dict]:
        """List all custom conversions for the ad account."""
        try:
            print("Fetching all custom conversions...")
            custom_conversions = self.account.get_custom_conversions(
                fields=[
                    CustomConversion.Field.id,
                    CustomConversion.Field.name,
                    CustomConversion.Field.custom_event_type,
                    CustomConversion.Field.rule,
                    CustomConversion.Field.creation_time,
                    CustomConversion.Field.last_fired_time,
                    CustomConversion.Field.pixel,
                    CustomConversion.Field.default_conversion_value,
                    CustomConversion.Field.description
                ]
            )
            
            conversions_list = []
            for conversion in custom_conversions:
                conversion_data = {
                    'id': conversion['id'],
                    'name': conversion['name'],
                    'event_type': conversion.get('custom_event_type'),
                    'rule': conversion.get('rule'),
                    'creation_time': conversion.get('creation_time'),
                    'last_fired_time': conversion.get('last_fired_time'),
                    'pixel_id': conversion.get('pixel', {}).get('id'),
                    'default_value': conversion.get('default_conversion_value'),
                    'description': conversion.get('description')
                }
                conversions_list.append(conversion_data)
            
            print(f"Found {len(conversions_list)} custom conversions")
            return conversions_list
            
        except FacebookRequestError as e:
            print(f"Facebook API Error listing custom conversions: {str(e)}")
            print(f"Error Code: {e.api_error_code()}")
            print(f"Error Message: {e.api_error_message()}")
            print(f"Error Type: {e.api_error_type()}")
            raise
        except Exception as e:
            print(f"Unexpected error listing custom conversions: {str(e)}")
            raise
    
    @backoff.on_exception(backoff.expo, FacebookRequestError, max_tries=5)
    def extract_ads_async(self, start_date: Union[datetime.date, None] = None, end_date: Union[datetime.date, None] = None) -> Tuple[List[Dict], List[Dict], Dict]:
        """Extract ads data and performance insights asynchronously for a given date range."""
        try:
            print("Starting ads data extraction...")
            ads_data = []
            performance_data = []
            creative_data = []  # New list for creative data
            status_info = {
                'status': 'IN_PROGRESS',
                'total_ads': 0,
                'processed_ads': 0,
                'errors': [],
                'response': ''
            }

            # Get the pixel ID
            pixel_id = self.get_pixel_id_from_account()
            if not pixel_id:
                print("No pixel found in account")
                status_info['errors'].append("No pixel found in account")
            else:
                print(f"Found pixel: {pixel_id}")

            # First get basic ad data
            print("Getting basic ad data...")
            basic_ads = self.extract_ads()
            if not basic_ads:
                print("No ads found in account")
                status_info['response'] = "No ads found in account"
                return [], [], status_info
            
            print(f"Found {len(basic_ads)} ads")
            status_info['total_ads'] = len(basic_ads)

            # Create insights params, including the time range
            time_range_param = {}
            if start_date and end_date:
                time_range_param = {
                    'since': start_date.strftime('%Y-%m-%d'),
                    'until': end_date.strftime('%Y-%m-%d')
                }

            params = {
                'level': 'ad',
                'fields': [
                    'ad_id',
                    'ad_name',
                    'adset_id',
                    'adset_name',
                    'campaign_id',
                    'campaign_name',
                    'impressions',
                    'clicks',
                    'spend',
                    'actions',
                    'action_values',
                    'date_start',
                    'date_stop'
                ],
                'action_attribution_windows': ['1d_click'],
                'filtering': [{
                    'field': 'action_type',
                    'operator': 'IN',
                    'value': ['offsite_conversion.fb_pixel_custom']
                }]
            }

            if time_range_param:
                params['time_range'] = time_range_param
                print(f"Using date range: {time_range_param}")

            # Get insights for each ad
            for basic_ad in basic_ads:
                try:
                    ad_id = basic_ad['id']
                    print(f"Getting insights for ad {ad_id}")
                    
                    # Get ad insights
                    ad = Ad(ad_id)
                    insights = ad.get_insights(params=params)
                    
                    if not insights:
                        print(f"No insights found for ad {ad_id}")
                        continue
                    
                    # Get creative details
                    creative_id = basic_ad.get('creative', {}).get('id')
                    if creative_id:
                        try:
                            creative = AdCreative(creative_id)
                            creative_details = creative.api_get(
                                fields=[
                                    'id',
                                    'title',
                                    'body',
                                    'image_url',
                                    'thumbnail_url',
                                    'object_story_spec',
                                    'call_to_action_type',
                                    'link_url'
                                ]
                            ).export_all_data()
                            creative_details['ad_id'] = ad_id  # Link creative to ad
                            creative_data.append(creative_details)
                            print(f"Got creative data for ad {ad_id}")
                        except Exception as e:
                            print(f"Error getting creative details for {creative_id}: {str(e)}")
                    
                    # Process insights
                    for insight in insights:
                        metrics = insight.export_all_data()
                        
                        # Format the data to match offsite_conversions.json structure
                        performance_metric = {
                            'ad_id': ad_id,
                            'ad_name': metrics.get('ad_name'),
                            'adset_id': metrics.get('adset_id'),
                            'adset_name': metrics.get('adset_name'),
                            'campaign_id': metrics.get('campaign_id'),
                            'campaign_name': metrics.get('campaign_name'),
                            'impressions': str(metrics.get('impressions', '0')),
                            'clicks': str(metrics.get('clicks', '0')),
                            'spend': str(metrics.get('spend', '0')),
                            'actions': [
                                action for action in metrics.get('actions', [])
                                if action.get('action_type') == 'offsite_conversion.fb_pixel_custom'
                            ],
                            'action_values': [
                                action for action in metrics.get('action_values', [])
                                if action.get('action_type') == 'offsite_conversion.fb_pixel_custom'
                            ],
                            'date_start': metrics.get('date_start'),
                            'date_stop': metrics.get('date_stop')
                        }
                        
                        print(f"Successfully processed ad {ad_id}: {performance_metric['ad_name']}")
                        
                        # Store the basic ad data
                        ads_data.append(basic_ad)
                        # Store the performance metrics in the format matching offsite_conversions.json
                        performance_data.append(performance_metric)
                        status_info['processed_ads'] += 1
                        
                except Exception as e:
                    error_msg = f"Error processing ad {ad_id}: {str(e)}"
                    print(error_msg)
                    status_info['errors'].append(error_msg)
                    continue

            # Save creative data to a separate file
            if creative_data:
                os.makedirs('output', exist_ok=True)
                with open('output/ad_creatives.json', 'w') as f:
                    json.dump(creative_data, f, indent=2)
                print(f"Saved {len(creative_data)} creative records to output/ad_creatives.json")

            status_info['status'] = 'COMPLETED'
            status_info['response'] = f"Successfully processed {status_info['processed_ads']} of {status_info['total_ads']} ads"
            return ads_data, performance_data, status_info

        except Exception as e:
            error_msg = f"Error in extract_ads_async: {str(e)}"
            print(error_msg)
            status_info['status'] = 'ERROR'
            status_info['response'] = error_msg
            status_info['errors'].append(error_msg)
            return [], [], status_info
    
    def get_extraction_status(self, request_set_id: str) -> Dict:
        """Get the status of an async extraction."""
        params = {
            'fields': [
                'id',
                'name',
                'owner_id',
                'is_completed',
                'total_count',
                'initial_count',
                'in_progress_count',
                'success_count',
                'error_count',
                'canceled_count'
            ]
        }
        result = self.account.get_async_ad_request_sets(params=params)
        for request_set in result:
            if request_set['id'] == request_set_id:
                return request_set.export_data()
        return {}
    
    def get_extraction_results(self, request_set_id: str) -> List[Dict]:
        """Get the results of an async extraction."""
        params = {
            'fields': ['id', 'status', 'result']
        }
        results = self.account.get_async_ad_requests(params=params)
        
        # Process the results
        ads = []
        for result in results:
            if result.get('async_request_set') == request_set_id and result.get('status') == 'SUCCESS':
                ads.extend(result.get('result', {}).get('data', []))
        
        return [ad.export_all_data() for ad in ads]
    
    def cancel_extraction(self, request_set_id: str) -> bool:
        """Cancel an async extraction if it hasn't been processed yet."""
        params = {'id': request_set_id}
        return self.account.delete_async_ad_request_sets(params=params)
    
    def extract_pixel_events(self, pixel_id: str = None) -> Dict:
        """Extract Meta Pixel event data."""
        try:
            if not pixel_id:
                # Get pixel ID from custom conversion if not provided
                conversion_data = self.extract_custom_conversion_data()
                if conversion_data and conversion_data.get('pixel_id'):
                    pixel_id = conversion_data['pixel_id']
                else:
                    print("No pixel ID found")
                    return None

            print(f"Fetching pixel events for pixel ID: {pixel_id}")
            
            # Initialize AdsPixel object
            pixel = AdsPixel(pixel_id, api=self.api)
            
            # Get pixel details
            pixel_details = pixel.api_get(fields=[
                AdsPixel.Field.name,
                AdsPixel.Field.code,
                AdsPixel.Field.creation_time,
                AdsPixel.Field.last_fired_time,
                AdsPixel.Field.first_party_cookie_status,
                AdsPixel.Field.data_use_setting,
                AdsPixel.Field.event_stats
            ])
            
            if not pixel_details:
                print("No pixel found with the provided ID")
                return None
            
            # Get pixel stats with insights
            end_time = datetime.now()
            start_time = end_time - timedelta(days=30)
            
            # Get insights for the pixel using the account
            stats = self.account.get_insights(
                fields=[
                    'actions',
                    'action_values',
                    'conversion_values',
                    'conversions',
                    'date_start',
                    'date_stop'
                ],
                params={
                    "time_range": {
                        "since": start_time.strftime("%Y-%m-%d"),
                        "until": end_time.strftime("%Y-%m-%d")
                    },
                    "level": "ad",
                    "action_attribution_windows": ["1d_click", "7d_click", "1d_view", "7d_view"],
                    "action_report_time": "conversion",
                    "filtering": [{
                        "field": "action_type",
                        "operator": "IN",
                        "value": ["offsite_conversion.fb_pixel_custom"]
                    }]
                }
            )
            
            pixel_data = {
                'pixel_id': pixel_id,
                'name': pixel_details.get('name'),
                'code': pixel_details.get('code'),
                'creation_time': pixel_details.get('creation_time'),
                'last_fired_time': pixel_details.get('last_fired_time'),
                'first_party_cookie_status': pixel_details.get('first_party_cookie_status'),
                'data_use_setting': pixel_details.get('data_use_setting'),
                'event_stats': pixel_details.get('event_stats'),
                'stats': stats
            }
            
            print(f"Successfully fetched pixel data:")
            print(f"Name: {pixel_data['name']}")
            print(f"Last fired: {pixel_data['last_fired_time']}")
            if stats:
                print(f"Stats records: {len(stats)}")
            if pixel_data.get('event_stats'):
                print(f"Event stats: {pixel_data['event_stats']}")
            
            return pixel_data
            
        except FacebookRequestError as e:
            print(f"Facebook API Error fetching pixel events: {str(e)}")
            print(f"Error Code: {e.api_error_code()}")
            print(f"Error Message: {e.api_error_message()}")
            print(f"Error Type: {e.api_error_type()}")
            raise
        except Exception as e:
            print(f"Unexpected error fetching pixel events: {str(e)}")
            raise

    def get_pixel_id_from_account(self) -> Optional[str]:
        """Get the pixel ID associated with the ad account."""
        try:
            pixels = self.account.get_ads_pixels(fields=['id', 'name'])
            if pixels:
                pixel = pixels[0]  # Get the first pixel
                print(f"Found pixel: {pixel['name']} (ID: {pixel['id']})")
                return pixel['id']
            else:
                print("No pixels found for this account")
                return None
        except Exception as e:
            print(f"Error getting pixel ID: {str(e)}")
            return None 