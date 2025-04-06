"""
Data loaders for Facebook Ads data.
"""
import sqlite3
from typing import Dict, List
import json
from datetime import datetime

class FacebookAdsLoader:
    """Handles loading of Facebook Ads data into the database."""
    
    def __init__(self, db_path: str):
        """Initialize the loader with database path."""
        self.db_path = db_path
        self._init_db()
    
    def _init_db(self):
        """Initialize the database with required tables."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Create ads table
            cursor.execute("""
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
                    daily_budget REAL,
                    lifetime_budget REAL,
                    amount_spent REAL,
                    budget_remaining REAL,
                    impressions INTEGER,
                    clicks INTEGER,
                    ctr REAL,
                    reach INTEGER,
                    frequency REAL,
                    targeting TEXT,
                    placement TEXT,
                    optimization_goal TEXT,
                    start_time TEXT,
                    end_time TEXT,
                    review_status TEXT,
                    review_feedback TEXT,
                    delivery_info TEXT
                )
            """)
            
            # Create performance_metrics table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS performance_metrics (
                    ad_id TEXT,
                    timestamp TEXT,
                    impressions INTEGER,
                    clicks INTEGER,
                    spend REAL,
                    reach INTEGER,
                    frequency REAL,
                    ctr REAL,
                    cpc REAL,
                    cpm REAL,
                    actions JSON,
                    conversion_values REAL,
                    conversions INTEGER,
                    website_ctr REAL,
                    website_purchase_roas REAL,
                    cost_per_conversion REAL,
                    conversion_rate_ranking TEXT,
                    PRIMARY KEY (ad_id, timestamp),
                    FOREIGN KEY (ad_id) REFERENCES ads(id)
                )
            """)
            
            # Create pixels table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pixels (
                    pixel_id TEXT PRIMARY KEY,
                    name TEXT,
                    last_synced TEXT
                )
            """)
            
            # Create pixel_events table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pixel_events (
                    event_id TEXT PRIMARY KEY,
                    pixel_id TEXT,
                    event_name TEXT,
                    event_time TEXT,
                    value REAL,
                    url TEXT,
                    custom_data JSON,
                    user_data JSON,
                    FOREIGN KEY (pixel_id) REFERENCES pixels(pixel_id)
                )
            """)
            
            # Create daily_pixel_stats table for aggregated daily metrics
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS daily_pixel_stats (
                    pixel_id TEXT,
                    date TEXT,
                    event_name TEXT,
                    event_count INTEGER,
                    total_value REAL,
                    PRIMARY KEY (pixel_id, date, event_name),
                    FOREIGN KEY (pixel_id) REFERENCES pixels(pixel_id)
                )
            """)
            
            # Create custom_conversions table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS custom_conversions (
                    conversion_id TEXT PRIMARY KEY,
                    name TEXT,
                    event_type TEXT,
                    rule JSON,
                    creation_time TEXT,
                    last_fired_time TEXT,
                    pixel_id TEXT,
                    default_value REAL
                )
            """)
            
            # Create conversion_stats table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS conversion_stats (
                    conversion_id TEXT,
                    date TEXT,
                    value INTEGER,
                    PRIMARY KEY (conversion_id, date),
                    FOREIGN KEY (conversion_id) REFERENCES custom_conversions(conversion_id)
                )
            """)
            
            conn.commit()
    
    def load_ads(self, transformed_ads: List[Dict]):
        """Load transformed ads data into the database."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            for ad in transformed_ads:
                # Convert creative data to JSON string
                creative_data = json.dumps(ad.get('creative', {}))
                
                cursor.execute("""
                    INSERT OR REPLACE INTO ads (
                        id, name, status, campaign_id, adset_id,
                        created_time, updated_time, effective_status,
                        last_synced, creative_data, daily_budget,
                        lifetime_budget, amount_spent, budget_remaining,
                        impressions, clicks, ctr, reach, frequency,
                        targeting, placement, optimization_goal,
                        start_time, end_time, review_status,
                        review_feedback, delivery_info
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    ad.get('id'),
                    ad.get('name'),
                    ad.get('status'),
                    ad.get('campaign_id'),
                    ad.get('adset_id'),
                    ad.get('created_time'),
                    ad.get('updated_time'),
                    ad.get('effective_status'),
                    datetime.now().isoformat(),
                    creative_data,
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
            
            conn.commit()

    def load_performance_metrics(self, metrics_data: List[Dict]):
        """Load performance metrics data into the database."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            for metric in metrics_data:
                # Convert metrics to proper numeric types
                impressions = int(metric.get('impressions', 0))
                clicks = int(metric.get('clicks', 0))
                spend = float(metric.get('spend', 0))
                reach = int(metric.get('reach', 0))
                frequency = float(metric.get('frequency', 0))
                
                # Calculate derived metrics
                ctr = (clicks / impressions * 100) if impressions > 0 else 0
                cpc = (spend / clicks) if clicks > 0 else 0
                cpm = (spend / impressions * 1000) if impressions > 0 else 0
                
                cursor.execute("""
                    INSERT OR REPLACE INTO performance_metrics (
                        ad_id, timestamp, impressions, clicks, spend,
                        reach, frequency, ctr, cpc, cpm, actions,
                        conversion_values, conversions, website_ctr, website_purchase_roas,
                        cost_per_conversion, conversion_rate_ranking
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    metric.get('ad_id'),
                    metric.get('timestamp', datetime.now().isoformat()),
                    impressions,
                    clicks,
                    spend,
                    reach,
                    frequency,
                    ctr,
                    cpc,
                    cpm,
                    json.dumps(metric.get('actions', [])),
                    float(metric.get('conversion_values', 0)),
                    int(metric.get('conversions', 0)),
                    float(metric.get('website_ctr', 0)),
                    float(metric.get('website_purchase_roas', 0)),
                    float(metric.get('cost_per_conversion', 0)),
                    metric.get('conversion_rate_ranking', '')
                ))
            
            conn.commit()

    def load_custom_conversion(self, conversion_data: Dict):
        """Load custom conversion data into the database."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Insert conversion details
            cursor.execute("""
                INSERT OR REPLACE INTO custom_conversions (
                    conversion_id, name, event_type, rule,
                    creation_time, last_fired_time, pixel_id, default_value
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                conversion_data['id'],
                conversion_data['name'],
                conversion_data['event_type'],
                json.dumps(conversion_data['rule']),
                conversion_data['creation_time'],
                conversion_data['last_fired_time'],
                conversion_data['pixel_id'],
                conversion_data['default_value']
            ))
            
            # Insert conversion stats
            if conversion_data.get('stats'):
                for stat in conversion_data['stats']:
                    cursor.execute("""
                        INSERT OR REPLACE INTO conversion_stats (
                            conversion_id, date, value
                        ) VALUES (?, ?, ?)
                    """, (
                        conversion_data['id'],
                        stat['date_start'],
                        stat['value']
                    ))
            
            conn.commit()

    def load_pixel_data(self, pixel_data: Dict):
        """Load pixel data and events into the database."""
        if not pixel_data or 'pixel_id' not in pixel_data:
            print("No valid pixel data provided")
            return
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Update pixel record
            cursor.execute("""
                INSERT OR REPLACE INTO pixels (
                    pixel_id, name, last_synced
                ) VALUES (?, ?, ?)
            """, (
                pixel_data['pixel_id'],
                pixel_data.get('name', 'Facebook Pixel'),
                datetime.now().isoformat()
            ))
            
            # Process events
            if 'events' in pixel_data and pixel_data['events']:
                daily_stats = {}  # For aggregating daily statistics
                
                for event in pixel_data['events']:
                    # Store individual event
                    event_id = f"{event['event_name']}_{event['timestamp']}"
                    cursor.execute("""
                        INSERT OR REPLACE INTO pixel_events (
                            event_id, pixel_id, event_name, event_time,
                            value, url, custom_data, user_data
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        event_id,
                        pixel_data['pixel_id'],
                        event['event_name'],
                        event['timestamp'],
                        float(event.get('value', 0)),
                        event.get('url'),
                        json.dumps(event.get('custom_data', {})),
                        json.dumps(event.get('user_data', {}))
                    ))
                    
                    # Aggregate daily statistics
                    date = event['timestamp'].split('T')[0]
                    key = (date, event['event_name'])
                    if key not in daily_stats:
                        daily_stats[key] = {
                            'count': 0,
                            'value': 0
                        }
                    daily_stats[key]['count'] += 1
                    daily_stats[key]['value'] += float(event.get('value', 0))
                
                # Store daily aggregates
                for (date, event_name), stats in daily_stats.items():
                    cursor.execute("""
                        INSERT OR REPLACE INTO daily_pixel_stats (
                            pixel_id, date, event_name, event_count, total_value
                        ) VALUES (?, ?, ?, ?, ?)
                    """, (
                        pixel_data['pixel_id'],
                        date,
                        event_name,
                        stats['count'],
                        stats['value']
                ))
            
            conn.commit()
    
    def get_ads(self, limit: int = None) -> List[Dict]:
        """Retrieve ads from the database."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            query = "SELECT * FROM ads"
            if limit:
                query += f" LIMIT {limit}"
            
            cursor.execute(query)
            columns = [description[0] for description in cursor.description]
            ads = []
            
            for row in cursor.fetchall():
                ad_dict = dict(zip(columns, row))
                # Parse JSON fields
                for json_field in ['creative_data', 'targeting', 'delivery_info']:
                    if json_field in ad_dict and ad_dict[json_field]:
                        try:
                            ad_dict[json_field] = json.loads(ad_dict[json_field])
                        except:
                            ad_dict[json_field] = {}
                ads.append(ad_dict)
            
            return ads
    
    def get_performance_metrics(self, ad_id: str = None, start_date: str = None, end_date: str = None) -> List[Dict]:
        """Retrieve performance metrics from the database with optional filtering."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            query = "SELECT * FROM performance_metrics WHERE 1=1"
            params = []
            
            if ad_id:
                query += " AND ad_id = ?"
                params.append(ad_id)
            
            if start_date:
                query += " AND timestamp >= ?"
                params.append(start_date)
            
            if end_date:
                query += " AND timestamp <= ?"
                params.append(end_date)
            
            query += " ORDER BY timestamp DESC"
            
            cursor.execute(query, params)
            columns = [description[0] for description in cursor.description]
            metrics = []
            
            for row in cursor.fetchall():
                metric_dict = dict(zip(columns, row))
                metric_dict['actions'] = json.loads(metric_dict['actions'])
                metrics.append(metric_dict)
            
            return metrics
    
    def get_custom_conversion_stats(self) -> List[Dict]:
        """Retrieve custom conversion statistics from the database."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT cs.date, cs.value, cc.name
                FROM conversion_stats cs
                JOIN custom_conversions cc ON cs.conversion_id = cc.conversion_id
                ORDER BY cs.date
            """)
            
            results = []
            for row in cursor.fetchall():
                results.append({
                    'date': row[0],
                    'value': row[1],
                    'conversion_name': row[2]
                })
            
            return results
    
    def get_pixel_stats(self, start_date: str = None, end_date: str = None) -> List[Dict]:
        """Retrieve pixel statistics from the database."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            query = """
                SELECT 
                    ps.date,
                    ps.event_name,
                    ps.event_count,
                    ps.total_value,
                    p.name as pixel_name
                FROM daily_pixel_stats ps
                JOIN pixels p ON ps.pixel_id = p.pixel_id
                WHERE 1=1
            """
            params = []
            
            if start_date:
                query += " AND ps.date >= ?"
                params.append(start_date)
            
            if end_date:
                query += " AND ps.date <= ?"
                params.append(end_date)
            
            query += " ORDER BY ps.date, ps.event_name"
            
            cursor.execute(query, params)
            
            results = []
            for row in cursor.fetchall():
                results.append({
                    'date': row[0],
                    'event_name': row[1],
                    'event_count': row[2],
                    'total_value': row[3],
                    'pixel_name': row[4]
                })
            
            return results
    
    def clear_ads(self):
        """Clear all ads from the database."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM ads")
            cursor.execute("DELETE FROM performance_metrics")
            conn.commit()

    def _create_tables(self):
        """Create necessary database tables if they don't exist."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Drop existing tables to ensure schema consistency
            cursor.execute("DROP TABLE IF EXISTS conversion_metrics")
            cursor.execute("DROP TABLE IF EXISTS conversions")
            cursor.execute("DROP TABLE IF EXISTS performance_metrics")
            cursor.execute("DROP TABLE IF EXISTS ads")
            
            # Create ads table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS ads (
                    id TEXT PRIMARY KEY,
                    name TEXT,
                    campaign_id TEXT,
                    adset_id TEXT,
                    status TEXT,
                    created_time TEXT,
                    updated_time TEXT,
                    effective_status TEXT,
                    creative_data TEXT,
                    pixel_id TEXT
                )
            """)
            
            # Create performance metrics table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS performance_metrics (
                    ad_id TEXT,
                    timestamp TEXT,
                    impressions INTEGER,
                    clicks INTEGER,
                    spend REAL,
                    reach INTEGER,
                    frequency REAL,
                    ctr REAL,
                    cpc REAL,
                    cpm REAL,
                    conversions INTEGER,
                    conversion_value REAL,
                    PRIMARY KEY (ad_id, timestamp),
                    FOREIGN KEY (ad_id) REFERENCES ads(id)
                )
            """)
            
            # Create conversions table for ad-level conversion data
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS conversions (
                    ad_id TEXT,
                    event_name TEXT,
                    value REAL,
                    action_type TEXT,
                    timestamp TEXT,
                    PRIMARY KEY (ad_id, event_name, timestamp),
                    FOREIGN KEY (ad_id) REFERENCES ads(id)
                )
            """)
            
            # Create conversion metrics table for detailed conversion tracking
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS conversion_metrics (
                    ad_id TEXT,
                    timestamp TEXT,
                    event_name TEXT,
                    value REAL,
                    action_type TEXT,
                    PRIMARY KEY (ad_id, timestamp, event_name),
                    FOREIGN KEY (ad_id) REFERENCES ads(id)
                )
            """)
            
            conn.commit()

    def load_ads_data(self, ads_data: List[Dict], performance_data: List[Dict]) -> None:
        """Load ads data and performance metrics into the database."""
        try:
            # Create tables if they don't exist
            self._create_tables()
            
            # Begin transaction
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Load ads data
                for ad in ads_data:
                    # Convert creative data to JSON string
                    creative_data = json.dumps(ad.get('creative', {}))
                    
                    # Insert ad data
                    cursor.execute("""
                        INSERT OR REPLACE INTO ads (
                            id, name, campaign_id, adset_id, status,
                            created_time, updated_time, effective_status,
                            creative_data, pixel_id
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        ad['id'],
                        ad['name'],
                        ad['campaign_id'],
                        ad['adset_id'],
                        ad.get('status', 'UNKNOWN'),
                        ad.get('created_time', datetime.now().isoformat()),
                        ad.get('updated_time', datetime.now().isoformat()),
                        ad.get('effective_status', 'UNKNOWN'),
                        creative_data,
                        ad.get('pixel_id')
                    ))
                    
                    # Insert conversion data
                    if 'conversions' in ad:
                        for conv in ad['conversions']:
                            cursor.execute("""
                                INSERT OR REPLACE INTO conversions (
                                    ad_id, event_name, value, action_type,
                                    timestamp
                                ) VALUES (?, ?, ?, ?, datetime('now'))
                            """, (
                                ad['id'],
                                conv.get('event_name'),
                                conv.get('value', 0),
                                conv.get('action_type')
                            ))
                
                # Load performance data
                for metric in performance_data:
                    cursor.execute("""
                        INSERT OR REPLACE INTO performance_metrics (
                            ad_id, timestamp, impressions, clicks, spend,
                            reach, frequency, ctr, cpc, cpm,
                            conversions, conversion_value
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        metric['ad_id'],
                        metric.get('date_start', datetime.now().strftime('%Y-%m-%d')),
                        int(metric.get('impressions', 0)),
                        int(metric.get('clicks', 0)),
                        float(metric.get('spend', 0)),
                        int(metric.get('reach', 0)),
                        float(metric.get('frequency', 0)),
                        float(metric.get('ctr', 0)),
                        float(metric.get('cpc', 0)),
                        float(metric.get('cpm', 0)),
                        len(metric.get('actions', [])),
                        sum(float(action.get('value', 0)) for action in metric.get('action_values', []))
                    ))
                    
                    # Insert detailed conversion data
                    if 'actions' in metric:
                        for action in metric['actions']:
                            if action.get('action_type') == 'offsite_conversion.fb_pixel_custom':
                                cursor.execute("""
                                    INSERT OR REPLACE INTO conversion_metrics (
                                        ad_id, timestamp, event_name,
                                        value, action_type
                                    ) VALUES (?, ?, ?, ?, ?)
                                """, (
                                    metric['ad_id'],
                                    metric.get('date_start', datetime.now().strftime('%Y-%m-%d')),
                                    action.get('action_type'),
                                    float(action.get('value', 0)),
                                    action.get('action_type')
                                ))
                
                conn.commit()
                print(f"Successfully loaded {len(ads_data)} ads and {len(performance_data)} performance records")
                
        except Exception as e:
            print(f"Error in load_ads_data: {str(e)}")
            raise 