"""
Data transformers for Facebook Ads data.
"""
from typing import Dict, List, Optional, Tuple
from datetime import datetime

class FacebookAdsTransformer:
    """Handles transformation of Facebook Ads data."""
    
    @staticmethod
    def transform_creative(creative_data: Dict) -> Dict:
        """Transform creative data to extract useful information."""
        if not creative_data:
            return {}
            
        transformed = {}
        
        # Extract basic fields
        transformed['creative_id'] = creative_data.get('id')
        transformed['headline'] = creative_data.get('title', '')
        transformed['text'] = creative_data.get('body', '')
        transformed['image_url'] = (
            creative_data.get('image_url') or 
            creative_data.get('thumbnail_url') or 
            creative_data.get('template_url')
        )
        transformed['link_url'] = creative_data.get('link_url', '')
        transformed['call_to_action'] = creative_data.get('call_to_action_type', '')
        
        # Extract from object story spec
        story_spec = creative_data.get('object_story_spec', {})
        if story_spec:
            link_data = story_spec.get('link_data', {})
            if link_data:
                if not transformed['headline']:
                    transformed['headline'] = link_data.get('name', '')
                if not transformed['text']:
                    transformed['text'] = link_data.get('message', '')
                if not transformed['image_url'] and link_data.get('image_crops'):
                    transformed['image_url'] = link_data['image_crops'].get('100x100', {}).get('url', '')
        
        # Extract from asset feed spec
        asset_feed = creative_data.get('asset_feed_spec', {})
        if asset_feed:
            if not transformed['headline']:
                for item in asset_feed.get('titles', []):
                    if isinstance(item, dict) and item.get('text'):
                        transformed['headline'] = item['text']
                        break
            if not transformed['text']:
                for item in asset_feed.get('bodies', []):
                    if isinstance(item, dict) and item.get('text'):
                        transformed['text'] = item['text']
                        break
        
        return transformed
    
    @staticmethod
    def transform_ad(ad_data: Dict) -> Dict:
        """Transform ad data with its creative information."""
        transformed = {
            'id': ad_data.get('id'),
            'name': ad_data.get('name'),
            'status': ad_data.get('status'),
            'campaign_id': ad_data.get('campaign_id'),
            'adset_id': ad_data.get('adset_id'),
            'created_time': ad_data.get('created_time'),
            'updated_time': ad_data.get('updated_time'),
            'effective_status': ad_data.get('effective_status'),
            'last_synced': datetime.now().isoformat(),
            
            # Budget fields
            'daily_budget': float(ad_data.get('daily_budget', 0)),
            'lifetime_budget': float(ad_data.get('lifetime_budget', 0)),
            'amount_spent': float(ad_data.get('spend', 0)),
            'budget_remaining': float(ad_data.get('budget_remaining', 0)),
            
            # Performance metrics
            'impressions': int(ad_data.get('impressions', 0)),
            'clicks': int(ad_data.get('clicks', 0)),
            'ctr': float(ad_data.get('ctr', 0)),
            'reach': int(ad_data.get('reach', 0)),
            'frequency': float(ad_data.get('frequency', 0)),
            
            # Targeting and placement
            'targeting': ad_data.get('targeting', {}),
            'placement': ad_data.get('placement'),
            'optimization_goal': ad_data.get('optimization_goal'),
            
            # Scheduling
            'start_time': ad_data.get('start_time'),
            'end_time': ad_data.get('end_time'),
            
            # Review and delivery
            'review_status': ad_data.get('review_status'),
            'review_feedback': ad_data.get('review_feedback'),
            'delivery_info': ad_data.get('delivery_info', {})
        }
        
        # Transform creative data
        creative = ad_data.get('creative', {})
        if isinstance(creative, dict):
            transformed['creative'] = FacebookAdsTransformer.transform_creative(creative)
        else:
            transformed['creative'] = {}
        
        return transformed

    @staticmethod
    def transform_performance_metrics(ad_data: Dict) -> Dict:
        """Transform performance metrics data."""
        return {
            'ad_id': ad_data.get('id'),
            'timestamp': datetime.now().isoformat(),
            'impressions': ad_data.get('impressions', 0),
            'clicks': ad_data.get('clicks', 0),
            'spend': float(ad_data.get('spend', 0)),
            'reach': ad_data.get('reach', 0),
            'frequency': float(ad_data.get('frequency', 0)),
            'actions': ad_data.get('actions', [])
        }
    
    @staticmethod
    def transform_ads_batch(ads_data: List[Dict]) -> List[Dict]:
        """Transform a batch of ads."""
        transformed_ads = []
        
        for ad in ads_data:
            try:
                transformed_ad = FacebookAdsTransformer.transform_ad(ad)
                transformed_ads.append(transformed_ad)
            except Exception as e:
                print(f"Error transforming ad {ad.get('id', 'unknown')}: {str(e)}")
                continue
        
        return transformed_ads 