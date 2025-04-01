import sqlite3
from typing import Dict, List, Optional
import json
from datetime import datetime

class AdDatabase:
    def __init__(self, db_path: str = "facebook_ads.db"):
        self.db_path = db_path
        self.init_db()
    
    def init_db(self):
        """Initialize the database with required tables."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Create ads table with enhanced fields
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS ads (
                    id TEXT PRIMARY KEY,
                    name TEXT,
                    status TEXT,
                    campaign_id TEXT,
                    adset_id TEXT,
                    created_time TEXT,
                    updated_time TEXT,
                    effective_status TEXT,
                    last_synced TEXT,
                    creative_data TEXT,
                    
                    -- Budget fields
                    daily_budget REAL,
                    lifetime_budget REAL,
                    amount_spent REAL,
                    budget_remaining REAL,
                    
                    -- Performance metrics
                    impressions INTEGER,
                    clicks INTEGER,
                    ctr REAL,
                    reach INTEGER,
                    frequency REAL,
                    
                    -- Targeting and placement
                    targeting TEXT,
                    placement TEXT,
                    optimization_goal TEXT,
                    
                    -- Scheduling
                    start_time TEXT,
                    end_time TEXT,
                    
                    -- Review and delivery
                    review_status TEXT,
                    review_feedback TEXT,
                    delivery_info TEXT
                )
            ''')
            
            # Create metadata table for tracking last refresh
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            ''')
            
            conn.commit()
    
    def store_ads(self, ads: List[Dict]):
        """Store or update ads in the database."""
        print("\nStoring ads in database...")
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            current_time = datetime.now().isoformat()
            
            for ad in ads:
                try:
                    print(f"\nStoring ad: {ad.get('name')} (ID: {ad.get('id')})")
                    # Ensure detailed_creatives exists and is a list
                    detailed_creatives = ad.get('detailed_creatives', [])
                    if not isinstance(detailed_creatives, list):
                        print(f"Warning: detailed_creatives is not a list, converting from {type(detailed_creatives)}")
                        detailed_creatives = []
                    
                    print(f"Number of creatives: {len(detailed_creatives)}")
                    detailed_creatives_json = json.dumps(detailed_creatives)
                    
                    cursor.execute('''
                        INSERT OR REPLACE INTO ads (
                            id, name, status, campaign_id, adset_id,
                            created_time, updated_time, effective_status,
                            last_synced, creative_data,
                            daily_budget, lifetime_budget, amount_spent,
                            budget_remaining, impressions, clicks, ctr,
                            reach, frequency, targeting, placement,
                            optimization_goal, start_time, end_time,
                            review_status, review_feedback, delivery_info
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        ad.get('id'),
                        ad.get('name'),
                        ad.get('status'),
                        ad.get('campaign_id'),
                        ad.get('adset_id'),
                        ad.get('created_time'),
                        ad.get('updated_time'),
                        ad.get('effective_status'),
                        current_time,
                        detailed_creatives_json,
                        ad.get('daily_budget'),
                        ad.get('lifetime_budget'),
                        ad.get('amount_spent'),
                        ad.get('budget_remaining'),
                        ad.get('impressions'),
                        ad.get('clicks'),
                        ad.get('ctr'),
                        ad.get('reach'),
                        ad.get('frequency'),
                        json.dumps(ad.get('targeting', {})),
                        ad.get('placement'),
                        ad.get('optimization_goal'),
                        ad.get('start_time'),
                        ad.get('end_time'),
                        ad.get('review_status'),
                        ad.get('review_feedback'),
                        json.dumps(ad.get('delivery_info', {}))
                    ))
                    print("Successfully stored ad in database")
                except Exception as e:
                    print(f"Error storing ad {ad.get('id')}: {str(e)}")
                    continue  # Skip this ad if there's an error
            
            # Update last refresh time
            cursor.execute('''
                INSERT OR REPLACE INTO metadata (key, value)
                VALUES ('last_refresh', ?)
            ''', (current_time,))
            
            conn.commit()
            print("\nFinished storing all ads in database")
    
    def get_all_ads(self) -> List[Dict]:
        """Retrieve all ads from the database."""
        print("\nRetrieving ads from database...")
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute('SELECT * FROM ads')
            ads = []
            for row in cursor.fetchall():
                try:
                    ad_dict = dict(row)
                    print(f"\nLoading ad: {ad_dict.get('name')} (ID: {ad_dict.get('id')})")
                    
                    # Parse JSON fields
                    ad_dict['detailed_creatives'] = json.loads(ad_dict['creative_data'])
                    del ad_dict['creative_data']  # Remove the JSON string version
                    
                    # Parse other JSON fields
                    if ad_dict.get('targeting'):
                        ad_dict['targeting'] = json.loads(ad_dict['targeting'])
                    if ad_dict.get('delivery_info'):
                        ad_dict['delivery_info'] = json.loads(ad_dict['delivery_info'])
                    
                    ads.append(ad_dict)
                    print("Successfully loaded ad from database")
                except Exception as e:
                    print(f"Error loading ad {row['id']}: {str(e)}")
                    continue  # Skip this ad if there's an error
            
            print(f"\nFinished loading {len(ads)} ads from database")
            return ads
    
    def get_last_refresh_time(self) -> Optional[str]:
        """Get the timestamp of the last data refresh."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT value FROM metadata WHERE key = "last_refresh"')
            result = cursor.fetchone()
            return result[0] if result else None 