"""
Example usage of Facebook Ads API for ad operations.
"""
from typing import Dict, List, Optional
from facebook_business.adobjects.ad import Ad
from ..fb_client import FacebookAdsClient

def list_ads(fields: Optional[List[str]] = None) -> None:
    """List all ads in the ad account."""
    client = FacebookAdsClient()
    try:
        ads = client.get_ads(fields)
        
        print("\nAds:")
        print("-" * 50)
        for ad in ads:
            print("Ad Details:")
            for key, value in ad.items():
                print(f"{key}: {value}")
            print("-" * 50)
    except Exception as e:
        print(f"Error listing ads: {str(e)}")
        raise

def pause_ad(ad_id: str) -> Dict:
    """
    Pause a running ad.
    
    Args:
        ad_id: ID of the ad to pause
    
    Returns:
        Dictionary containing the updated ad data
    """
    client = FacebookAdsClient()
    try:
        ad = client.update_ad_status(ad_id, Ad.Status.paused)
        
        print("\nPaused Ad:")
        print("-" * 50)
        for key, value in ad.items():
            print(f"{key}: {value}")
        
        return ad
    except Exception as e:
        print(f"Error pausing ad: {str(e)}")
        raise

def activate_ad(ad_id: str) -> Dict:
    """
    Activate a paused ad.
    
    Args:
        ad_id: ID of the ad to activate
    
    Returns:
        Dictionary containing the updated ad data
    """
    client = FacebookAdsClient()
    try:
        ad = client.update_ad_status(ad_id, Ad.Status.active)
        
        print("\nActivated Ad:")
        print("-" * 50)
        for key, value in ad.items():
            print(f"{key}: {value}")
        
        return ad
    except Exception as e:
        print(f"Error activating ad: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        # Example usage
        print("1. Listing all ads:")
        list_ads()
        
        # Note: Replace with actual ad ID from your account
        example_ad_id = "123456789"
        
        print("\n2. Pausing an ad:")
        try:
            paused_ad = pause_ad(example_ad_id)
            
            print("\n3. Activating an ad:")
            activate_ad(example_ad_id)
        except Exception as e:
            print(f"\nNote: Ad operations failed because example_ad_id is not a real ID. Replace it with an actual ad ID from your account.")
    except Exception as e:
        print(f"Error in main execution: {str(e)}") 