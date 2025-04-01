"""
Example usage of Facebook Ads API for campaign operations.
"""
from typing import Dict, List, Optional
from facebook_business.adobjects.campaign import Campaign
from ..fb_client import FacebookAdsClient

def list_campaigns(fields: Optional[List[str]] = None) -> None:
    """List all campaigns in the ad account."""
    client = FacebookAdsClient()
    try:
        # Explicitly specify the fields we want to retrieve
        fields = [
            'id',
            'name',
            'status',
            'objective',
            'created_time',
            'start_time',
            'daily_budget',
            'lifetime_budget'
        ]
        campaigns = client.get_campaigns(fields)
        
        print("\nCampaigns:")
        print("-" * 50)
        if not campaigns:
            print("No campaigns found.")
            return
            
        print(f"Number of campaigns found: {len(campaigns)}")
        for i, campaign in enumerate(campaigns, 1):
            print(f"\nCampaign {i}:")
            if campaign.get('name'):
                print(f"Name: {campaign['name']}")
            if campaign.get('status'):
                print(f"Status: {campaign['status']}")
            if campaign.get('objective'):
                print(f"Objective: {campaign['objective']}")
            if campaign.get('created_time'):
                print(f"Created: {campaign['created_time']}")
            if campaign.get('daily_budget'):
                print(f"Daily Budget: {campaign['daily_budget']}")
            if campaign.get('lifetime_budget'):
                print(f"Lifetime Budget: {campaign['lifetime_budget']}")
            print("-" * 50)
    except Exception as e:
        print(f"Error listing campaigns: {str(e)}")
        raise

def create_campaign(
    name: str,
    objective: str = "OUTCOME_TRAFFIC",
    status: str = "PAUSED"
) -> Dict:
    """
    Create a new campaign with specified parameters.
    
    Args:
        name: Campaign name
        objective: Campaign objective (default: OUTCOME_TRAFFIC)
            Valid objectives: OUTCOME_LEADS, OUTCOME_SALES, OUTCOME_ENGAGEMENT,
            OUTCOME_AWARENESS, OUTCOME_TRAFFIC, OUTCOME_APP_PROMOTION
        status: Initial status (default: PAUSED)
    
    Returns:
        Dictionary containing the created campaign data
    """
    client = FacebookAdsClient()
    try:
        campaign = client.create_campaign(name, objective, status)
        
        print("\nCreated Campaign:")
        print("-" * 50)
        for key, value in campaign.items():
            print(f"{key}: {value}")
        
        return campaign
    except Exception as e:
        print(f"Error creating campaign: {str(e)}")
        raise

def update_campaign_status(campaign_id: str, new_status: str) -> Dict:
    """
    Update a campaign's status.
    
    Args:
        campaign_id: ID of the campaign to update
        new_status: New status (ACTIVE or PAUSED)
    
    Returns:
        Dictionary containing the updated campaign data
    """
    client = FacebookAdsClient()
    try:
        campaign = client.update_campaign(
            campaign_id,
            {Campaign.Field.status: new_status}
        )
        
        print("\nUpdated Campaign:")
        print("-" * 50)
        for key, value in campaign.items():
            print(f"{key}: {value}")
        
        return campaign
    except Exception as e:
        print(f"Error updating campaign: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        # Example usage
        print("1. Listing all campaigns:")
        list_campaigns()
        
        print("\n2. Creating a new test campaign:")
        new_campaign = create_campaign(
            name="Test Campaign via API",
            objective="OUTCOME_TRAFFIC"
        )
        
        if new_campaign and 'id' in new_campaign:
            print("\n3. Updating campaign status:")
            update_campaign_status(new_campaign['id'], "ACTIVE")
    except Exception as e:
        print(f"\nScript execution failed: {str(e)}") 