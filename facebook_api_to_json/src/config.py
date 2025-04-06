"""
Configuration module for Facebook Ads API integration.
Handles environment variables and Facebook API initialization.
"""
import os
from dotenv import load_dotenv
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount

# Load environment variables from .env file
load_dotenv()

# Facebook API credentials
FB_APP_ID = os.getenv('FB_APP_ID')
FB_APP_SECRET = os.getenv('FB_APP_SECRET')
FB_ACCESS_TOKEN = os.getenv('FB_ACCESS_TOKEN')
FB_AD_ACCOUNT_ID = os.getenv('FB_AD_ACCOUNT_ID')

def init_facebook_api():
    """Initialize Facebook Ads API with credentials from environment."""
    if not all([FB_APP_ID, FB_APP_SECRET, FB_ACCESS_TOKEN, FB_AD_ACCOUNT_ID]):
        raise ValueError(
            "Missing required Facebook API credentials. "
            "Please check your .env file."
        )
    
    # Initialize the Facebook Ads API
    FacebookAdsApi.init(FB_APP_ID, FB_APP_SECRET, FB_ACCESS_TOKEN)
    
    # Return the ad account instance
    return AdAccount(FB_AD_ACCOUNT_ID)

def get_ad_account():
    """Get the Facebook Ad Account instance."""
    try:
        return init_facebook_api()
    except ValueError as e:
        print(f"Configuration error: {e}")
        raise
    except Exception as e:
        print(f"Error initializing Facebook API: {e}")
        raise 