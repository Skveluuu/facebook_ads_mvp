"""
Facebook Ads API client wrapper providing common advertising operations.
"""
import os
import time
from typing import Dict, List, Optional, Union
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.adcreative import AdCreative
from facebook_business.exceptions import FacebookRequestError
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from dotenv import load_dotenv
import backoff
import requests
from requests.exceptions import RequestException

# Load environment variables
load_dotenv()

class FacebookAPIError(Exception):
    """Custom exception for Facebook API errors."""
    pass

def handle_facebook_error(e: FacebookRequestError) -> None:
    """Handle Facebook API errors with appropriate actions."""
    error_message = str(e)
    error_type = e.api_error_type() if hasattr(e, 'api_error_type') else 'Unknown'
    error_code = e.api_error_code() if hasattr(e, 'api_error_code') else 'Unknown'
    
    print(f"Facebook API Error: Type={error_type}, Code={error_code}, Message={error_message}")
    
    if error_code in ['17', '32', '4', '2']:  # Rate limiting errors
        wait_time = int(e.headers().get('Retry-After', 60))
        print(f"Rate limited. Waiting {wait_time} seconds...")
        time.sleep(wait_time)
    elif error_code in ['190', '102']:  # Authentication errors
        raise FacebookAPIError("Authentication failed. Please check your access token.")
    elif error_code in ['803', '100']:  # Permission errors
        raise FacebookAPIError("Permission denied. Please check your app permissions.")
    else:
        raise FacebookAPIError(f"Facebook API error: {error_message}")

@backoff.on_exception(
    backoff.expo,
    (FacebookRequestError, RequestException),
    max_tries=5,
    max_time=300
)
def retry_on_failure(func):
    """Decorator to retry API calls on failure with exponential backoff."""
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except FacebookRequestError as e:
            handle_facebook_error(e)
            raise
        except RequestException as e:
            print(f"Network error: {str(e)}")
            raise
    return wrapper

class FacebookAdsClient:
    """Wrapper for Facebook Ads API operations with robust error handling."""
    
    def __init__(self):
        """Initialize the client with an ad account."""
        self.api = self._init_api()
        self.account = self._get_ad_account()
        
    def _init_api(self) -> FacebookAdsApi:
        """Initialize Facebook API with proper error handling."""
        app_id = os.getenv('FB_APP_ID')
        app_secret = os.getenv('FB_APP_SECRET')
        access_token = os.getenv('FB_ACCESS_TOKEN')
        
        if not all([app_id, app_secret, access_token]):
            raise ValueError("Missing required Facebook API credentials in .env file")
        
        try:
            api = FacebookAdsApi.init(app_id, app_secret, access_token)
            return api
        except Exception as e:
            raise FacebookAPIError(f"Failed to initialize Facebook API: {str(e)}")
    
    def _get_ad_account(self) -> AdAccount:
        """Get ad account with error handling."""
        ad_account_id = os.getenv('FB_AD_ACCOUNT_ID')
        if not ad_account_id:
            raise ValueError("Missing FB_AD_ACCOUNT_ID in .env file")
        
        try:
            return AdAccount(ad_account_id)
        except Exception as e:
            raise FacebookAPIError(f"Failed to get ad account: {str(e)}")
    
    @retry_on_failure
    def get_ad_creatives(self, ad_id: str) -> List[Dict]:
        """Get all creatives for an ad with robust error handling."""
        try:
            ad = Ad(ad_id)
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
            ]
            
            creatives = ad.get_ad_creatives(fields=fields)
            return [creative.export_all_data() for creative in creatives]
        except FacebookRequestError as e:
            handle_facebook_error(e)
            return []
    
    @retry_on_failure
    def get_ads_with_creatives(self) -> List[Dict]:
        """Get all ads with their creative content."""
        try:
            # Get basic ad information
            print("Fetching ads from Facebook...")
            ads = self.account.get_ads(fields=[
                Ad.Field.id,
                Ad.Field.name,
                Ad.Field.status,
                Ad.Field.campaign_id,
                Ad.Field.adset_id,
                Ad.Field.created_time,
                Ad.Field.updated_time,
                Ad.Field.effective_status,
                Ad.Field.creative,  # This will get the creative object
            ])
            
            processed_ads = []
            for ad in ads:
                try:
                    print(f"\nProcessing ad: {ad['name']} (ID: {ad['id']})")
                    ad_data = ad.export_all_data()
                    
                    # Handle creative object directly
                    creative_obj = ad.get('creative')
                    print(f"Creative object: {creative_obj}")
                    
                    if creative_obj and hasattr(creative_obj, 'get_id'):
                        creative_id = creative_obj.get_id()
                        print(f"Getting creative details for ID: {creative_id}")
                        
                        # Fetch creative details
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
                        
                        creative_data = creative.api_get(fields=fields).export_all_data()
                        print(f"Got creative data: {creative_data}")
                        
                        # Extract headlines and text
                        headlines = []
                        
                        # Check title
                        if creative_data.get('title'):
                            headlines.append(creative_data['title'])
                        
                        # Check object story spec
                        story_spec = creative_data.get('object_story_spec', {})
                        if story_spec:
                            link_data = story_spec.get('link_data', {})
                            if link_data:
                                if link_data.get('name'):
                                    headlines.append(link_data['name'])
                                if link_data.get('message'):
                                    headlines.append(link_data['message'])
                                if link_data.get('description'):
                                    headlines.append(link_data['description'])
                        
                        # Add processed headlines
                        creative_data['all_headlines'] = list(set(headlines))
                        ad_data['detailed_creatives'] = [creative_data]
                    else:
                        print("No creative found for this ad")
                        ad_data['detailed_creatives'] = []
                    
                    processed_ads.append(ad_data)
                    
                except Exception as e:
                    print(f"Error processing ad {ad.get('id', 'unknown')}: {str(e)}")
                    continue
            
            print(f"\nProcessed {len(processed_ads)} ads")
            return processed_ads
            
        except FacebookRequestError as e:
            handle_facebook_error(e)
            return []
        except Exception as e:
            print(f"Unexpected error: {str(e)}")
            return []

    @retry_on_failure
    def get_campaigns(self, fields: Optional[List[str]] = None) -> List[Dict]:
        """Get all campaigns with retry logic."""
        if fields is None:
            fields = [
                Campaign.Field.id,
                Campaign.Field.name,
                Campaign.Field.status,
                Campaign.Field.objective,
                Campaign.Field.special_ad_categories,
                Campaign.Field.created_time,
                Campaign.Field.start_time,
                Campaign.Field.stop_time,
                Campaign.Field.daily_budget,
                Campaign.Field.lifetime_budget
            ]
        
        campaigns = self.account.get_campaigns(fields=fields)
        return [campaign.export_all_data() for campaign in campaigns]

    def create_campaign(
        self,
        name: str,
        objective: str,
        status: str = Campaign.Status.paused,
        special_ad_categories: Optional[List[str]] = None
    ) -> Dict:
        """
        Create a new campaign.
        
        Args:
            name: Campaign name
            objective: Campaign objective (e.g., LINK_CLICKS, CONVERSIONS)
            status: Campaign status (ACTIVE or PAUSED)
            special_ad_categories: List of special ad categories if applicable
            
        Returns:
            Dictionary with created campaign data
        """
        params = {
            Campaign.Field.name: name,
            Campaign.Field.objective: objective,
            Campaign.Field.status: status,
            # Set empty list as default for special_ad_categories
            Campaign.Field.special_ad_categories: special_ad_categories or ['NONE'],
        }
        
        try:
            campaign = self.account.create_campaign(params=params)
            return campaign.export_data()
        except FacebookRequestError as e:
            print(f"Error creating campaign: {e}")
            raise
    
    def update_campaign(self, campaign_id: str, updates: Dict) -> Dict:
        """
        Update an existing campaign.
        
        Args:
            campaign_id: ID of the campaign to update
            updates: Dictionary of fields to update
            
        Returns:
            Dictionary with updated campaign data
        """
        try:
            campaign = Campaign(campaign_id)
            campaign.api_update(params=updates)
            return campaign.api_get().export_data()
        except FacebookRequestError as e:
            print(f"Error updating campaign {campaign_id}: {e}")
            raise
    
    def get_ads(self, fields: Optional[List[str]] = None) -> List[Dict]:
        """
        Get all ads for the ad account.
        
        Args:
            fields: List of ad fields to retrieve
            
        Returns:
            List of ad data dictionaries
        """
        if fields is None:
            fields = [
                Ad.Field.id,
                Ad.Field.name,
                Ad.Field.status,
                Ad.Field.campaign_id,
                Ad.Field.adset_id,
                Ad.Field.creative,
            ]
        
        try:
            ads = []
            for ad in self.account.get_ads(fields=fields):
                ad_data = {}
                for field in fields:
                    try:
                        ad_data[field] = ad[field]
                    except (KeyError, TypeError):
                        ad_data[field] = None
                ads.append(ad_data)
            return ads
        except FacebookRequestError as e:
            print(f"Error fetching ads: {e}")
            raise
    
    def get_ad_creative(self, creative_id: str, fields: Optional[List[str]] = None) -> Dict:
        """
        Get creative content for an ad.
        
        Args:
            creative_id: ID of the creative to fetch
            fields: List of creative fields to retrieve
            
        Returns:
            Dictionary with creative data
        """
        if fields is None:
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
            ]
        
        try:
            creative = AdCreative(creative_id)
            return creative.api_get(fields=fields).export_data()
        except FacebookRequestError as e:
            print(f"Error fetching creative {creative_id}: {e}")
            raise
    
    def update_ad_status(self, ad_id: str, status: str) -> Dict:
        """
        Update an ad's status (pause/activate).
        
        Args:
            ad_id: ID of the ad to update
            status: New status (ACTIVE or PAUSED)
            
        Returns:
            Dictionary with updated ad data
        """
        try:
            ad = Ad(ad_id)
            ad.api_update(params={Ad.Field.status: status})
            return ad.api_get().export_data()
        except FacebookRequestError as e:
            print(f"Error updating ad {ad_id}: {e}")
            raise
    
    def get_detailed_ad_creative(self, creative_id: str) -> Dict:
        """
        Get detailed creative content including all possible headline variations and creative elements.
        
        Args:
            creative_id: ID of the creative to fetch
            
        Returns:
            Dictionary with detailed creative data
        """
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
            AdCreative.Field.template_url_spec,
            AdCreative.Field.video_id,
            AdCreative.Field.call_to_action_type,
            AdCreative.Field.asset_feed_spec,
            AdCreative.Field.image_crops,
            AdCreative.Field.instagram_actor_id,
            AdCreative.Field.instagram_permalink_url,
            AdCreative.Field.instagram_story_id,
            AdCreative.Field.link_og_id,
            AdCreative.Field.product_set_id,
            AdCreative.Field.recommender_settings,
            AdCreative.Field.messenger_sponsored_message,
        ]
        
        try:
            creative = AdCreative(creative_id)
            data = creative.api_get(fields=fields).export_data()
            
            # Extract headlines from various possible locations
            headlines = []
            
            # Check main title
            if data.get('title'):
                headlines.append(data['title'])
            
            # Check object story spec
            story_spec = data.get('object_story_spec', {})
            if story_spec:
                # Check link data
                link_data = story_spec.get('link_data', {})
                if link_data:
                    if link_data.get('name'):
                        headlines.append(link_data['name'])
                    if link_data.get('message'):
                        headlines.append(link_data['message'])
                    
                # Check page link
                page_link = story_spec.get('page_link', {})
                if page_link and page_link.get('name'):
                    headlines.append(page_link['name'])
                    
                # Check video data
                video_data = story_spec.get('video_data', {})
                if video_data and video_data.get('title'):
                    headlines.append(video_data['title'])
            
            # Check asset feed spec
            asset_feed = data.get('asset_feed_spec', {})
            if asset_feed:
                bodies = asset_feed.get('bodies', [])
                titles = asset_feed.get('titles', [])
                descriptions = asset_feed.get('descriptions', [])
                
                for item in bodies + titles + descriptions:
                    if isinstance(item, dict) and item.get('text'):
                        headlines.append(item['text'])
            
            # Add unique headlines to the data
            data['all_headlines'] = list(set(headlines))
            
            return data
        except FacebookRequestError as e:
            print(f"Error fetching creative {creative_id}: {e}")
            raise

    @backoff.on_exception(backoff.expo, FacebookRequestError, max_tries=5)
    def create_async_request_set(self, name: str, ad_specs: List[Dict], notification_mode: str = 'OFF') -> str:
        """
        Create a new async request set.
        
        Args:
            name: Name for the request set
            ad_specs: List of ad operation specifications
            notification_mode: Notification mode (default: 'OFF')
            
        Returns:
            ID of the created request set
        """
        params = {
            'name': name,
            'notification_mode': notification_mode,
            'ad_specs': ad_specs
        }
        request_set = self.account.create_async_ad_request_set(params=params)
        return request_set.get_id()

    @backoff.on_exception(backoff.expo, FacebookRequestError, max_tries=5)
    def get_async_request_set_status(self, request_set_id: str) -> Dict:
        """Get the status of an async request set."""
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

    @backoff.on_exception(backoff.expo, FacebookRequestError, max_tries=5)
    def get_async_request_results(self, request_set_id: str) -> List[Dict]:
        """Get results of completed requests in an async request set."""
        params = {
            'fields': ['id', 'status', 'result']
        }
        results = self.account.get_async_ad_requests(params=params)
        processed_results = []
        for result in results:
            if result.get('async_request_set') == request_set_id and result.get('status') == 'SUCCESS':
                processed_results.extend(result.get('result', {}).get('data', []))
        return processed_results

    @backoff.on_exception(backoff.expo, FacebookRequestError, max_tries=5)
    def cancel_async_request_set(self, request_set_id: str) -> bool:
        """Cancel an async request set if it hasn't been processed yet."""
        params = {'id': request_set_id}
        return self.account.delete_async_ad_request_sets(params=params)

    @backoff.on_exception(backoff.expo, FacebookRequestError, max_tries=5)
    def create_async_batch_request(self, name: str, requests: List[Dict]) -> None:
        """Create an async batch request for multiple operations."""
        params = {
            'name': name,
            'requests': requests
        }
        self.account.create_async_batch_request(params=params) 