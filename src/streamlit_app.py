"""
Streamlit app for displaying Facebook Ads content.
"""
import os
import sys
from datetime import datetime, timedelta
import streamlit as st
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adcreative import AdCreative
from typing import Dict, List, Optional
from dotenv import load_dotenv
import pandas as pd
import plotly.express as px
from etl.extractors import FacebookAdsExtractor
from etl.transformers import FacebookAdsTransformer
from etl.loaders import FacebookAdsLoader
import time
import sqlite3
import traceback
import json
from graphviz import Digraph
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Add src directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from fb_client import FacebookAdsClient
from database import AdDatabase

# Load environment variables
load_dotenv()

# Initialize database
db = AdDatabase()

# Initialize session state for ETL metrics
if 'etl_metrics' not in st.session_state:
    st.session_state.etl_metrics = {
        'last_run': None,
        'extraction_time': 0,
        'transformation_time': 0,
        'loading_time': 0,
        'total_ads': 0,
        'successful_ads': 0,
        'failed_ads': 0,
        'total_time': 0
    }

def should_refresh_data(refresh_interval_minutes: int = 60) -> bool:
    """Check if data should be refreshed based on last refresh time."""
    last_refresh = db.get_last_refresh_time()
    if not last_refresh:
        return True
    
    last_refresh_time = datetime.fromisoformat(last_refresh)
    return datetime.now() - last_refresh_time > timedelta(minutes=refresh_interval_minutes)

def refresh_ads_data(client: FacebookAdsClient):
    """Fetch fresh data from Facebook API and store in database."""
    try:
        # Get ads with detailed creative information
        ads = client.get_ads_with_creatives()
        
        # Store in database
        db.store_ads(ads)
        return ads
    except Exception as e:
        st.error(f"Error fetching ads: {str(e)}")
        return []

def display_headlines_tab(ads: List[Dict]):
    """Display headlines tab content."""
    st.header("Ad Headlines")
    
    for ad in ads:
        with st.expander(f"Ad: {ad.get('name', 'Unnamed Ad')}"):
            st.write("Status:", ad.get('status', 'Unknown'))
            st.write("Created:", ad.get('created_time', 'Unknown'))
            st.write("Last Updated:", ad.get('updated_time', 'Unknown'))
            
            # Display all headlines from creatives
            st.subheader("Headlines")
            for creative in ad.get('detailed_creatives', []):
                with st.container():
                    st.write("Creative ID:", creative.get('id'))
                    
                    # Display all found headlines
                    headlines = creative.get('all_headlines', [])
                    if headlines:
                        for idx, headline in enumerate(headlines, 1):
                            st.markdown(f"**Headline {idx}:** {headline}")
                    else:
                        st.write("No headlines found")
                    
                    st.divider()

def display_images_tab(ads: List[Dict]):
    """Display images tab content."""
    st.header("Ad Images")
    
    for ad in ads:
        with st.expander(f"Ad: {ad.get('name', 'Unnamed Ad')}"):
            st.write("Status:", ad.get('status', 'Unknown'))
            
            for creative in ad.get('detailed_creatives', []):
                with st.container():
                    st.write("Creative ID:", creative.get('id'))
                    
                    # Try different image sources
                    image_url = (
                        creative.get('image_url') or 
                        creative.get('thumbnail_url') or 
                        creative.get('template_url')
                    )
                    
                    if image_url:
                        st.image(image_url, use_container_width=True)
                    else:
                        # Try to get from object story spec
                        story_spec = creative.get('object_story_spec', {})
                        if story_spec:
                            # Check link data
                            link_data = story_spec.get('link_data', {})
                            if link_data:
                                image_data = link_data.get('image_crops', {})
                                if image_data:
                                    st.image(image_data.get('100x100', {}).get('url'), use_container_width=True)
                    
                    # Display video thumbnail if it's a video ad
                    if creative.get('video_id'):
                        st.write("Video Ad")
                        if creative.get('thumbnail_url'):
                            st.image(creative['thumbnail_url'], use_container_width=True)
                    
                    st.divider()

def display_text_tab(ads: List[Dict]):
    """Display text tab content."""
    st.header("Ad Text")
    
    for ad in ads:
        with st.expander(f"Ad: {ad.get('name', 'Unnamed Ad')}"):
            st.write("Status:", ad.get('status', 'Unknown'))
            
            for creative in ad.get('detailed_creatives', []):
                with st.container():
                    st.write("Creative ID:", creative.get('id'))
                    
                    # Display body text
                    st.subheader("Body Text")
                    body = creative.get('body')
                    if body:
                        st.write(body)
                    else:
                        # Try to get from object story spec
                        story_spec = creative.get('object_story_spec', {})
                        if story_spec:
                            link_data = story_spec.get('link_data', {})
                            if link_data and link_data.get('message'):
                                st.write(link_data['message'])
                            else:
                                st.write("No body text found")
                    
                    # Display CTA if available
                    cta = creative.get('call_to_action_type')
                    if cta:
                        st.write("Call to Action:", cta)
                    
                    # Display link URL if available
                    link_url = creative.get('link_url')
                    if link_url:
                        st.write("Link URL:", link_url)
                    
                    # Display object type
                    obj_type = creative.get('object_type')
                    if obj_type:
                        st.write("Object Type:", obj_type)
                    
                    st.divider()

def display_database_schema():
    """Display database schema information."""
    st.header("Database Schema")
    
    # Create expandable sections for each table
    with st.expander("Ads Table", expanded=True):
        st.markdown("""
        ### Ads Table
        Stores information about Facebook ads and their creative content.
        
        | Column Name | Type | Description |
        |------------|------|-------------|
        | id | TEXT PRIMARY KEY | Unique identifier for the ad |
        | name | TEXT | Name of the ad |
        | status | TEXT | Current status of the ad |
        | campaign_id | TEXT | ID of the campaign this ad belongs to |
        | adset_id | TEXT | ID of the ad set this ad belongs to |
        | created_time | TEXT | When the ad was created |
        | updated_time | TEXT | When the ad was last updated |
        | effective_status | TEXT | Effective status of the ad |
        | last_synced | TEXT | When the ad was last synced to our database |
        | creative_data | TEXT | JSON string containing creative content |
        | daily_budget | REAL | Daily budget for the ad |
        | lifetime_budget | REAL | Lifetime budget for the ad |
        | amount_spent | REAL | Total amount spent on the ad |
        | budget_remaining | REAL | Remaining budget for the ad |
        | impressions | INTEGER | Number of times the ad was shown |
        | clicks | INTEGER | Number of clicks on the ad |
        | ctr | REAL | Click-through rate (clicks/impressions) |
        | reach | INTEGER | Number of unique people who saw the ad |
        | frequency | REAL | Average number of times each person saw the ad |
        | targeting | TEXT | JSON string containing targeting information |
        | placement | TEXT | Where the ad is being shown |
        | optimization_goal | TEXT | What the ad is optimized for |
        | start_time | TEXT | When the ad started running |
        | end_time | TEXT | When the ad is scheduled to stop |
        | review_status | TEXT | Current review status of the ad |
        | review_feedback | TEXT | Feedback from ad review process |
        | delivery_info | TEXT | JSON string containing delivery information |
        """)
        
        # Display sample data with more columns
        st.subheader("Sample Data")
        with sqlite3.connect("facebook_ads.db") as conn:
            df = pd.read_sql_query("""
                SELECT 
                    id, name, status, created_time, 
                    amount_spent, impressions, clicks, ctr,
                    review_status
                FROM ads 
                LIMIT 5
            """, conn)
            if not df.empty:
                st.dataframe(df)
            else:
                st.info("No data available in the ads table")
    
    with st.expander("Metadata Table", expanded=True):
        st.markdown("""
        ### Metadata Table
        Stores system-level metadata and configuration.
        
        | Column Name | Type | Description |
        |------------|------|-------------|
        | key | TEXT PRIMARY KEY | Unique identifier for the metadata entry |
        | value | TEXT | Value associated with the key |
        """)
        
        # Display sample data
        st.subheader("Sample Data")
        with sqlite3.connect("facebook_ads.db") as conn:
            df = pd.read_sql_query("SELECT * FROM metadata", conn)
            if not df.empty:
                st.dataframe(df)
            else:
                st.info("No data available in the metadata table")
    
    # Display database statistics
    st.header("Database Statistics")
    col1, col2, col3 = st.columns(3)
    
    with sqlite3.connect("facebook_ads.db") as conn:
        cursor = conn.cursor()
        
        with col1:
            # Count of ads
            cursor.execute("SELECT COUNT(*) FROM ads")
            ads_count = cursor.fetchone()[0]
            st.metric("Total Ads", ads_count)
            
            # Count of active ads
            cursor.execute("SELECT COUNT(*) FROM ads WHERE status = 'ACTIVE'")
            active_ads = cursor.fetchone()[0]
            st.metric("Active Ads", active_ads)
        
        with col2:
            # Total spend
            cursor.execute("SELECT COALESCE(SUM(amount_spent), 0) FROM ads")
            total_spend = cursor.fetchone()[0]
            st.metric("Total Spend", f"${total_spend:.2f}")
            
            # Total impressions
            cursor.execute("SELECT COALESCE(SUM(impressions), 0) FROM ads")
            total_impressions = cursor.fetchone()[0]
            st.metric("Total Impressions", f"{total_impressions:,}")
        
        with col3:
            # Last sync time
            cursor.execute("SELECT value FROM metadata WHERE key = 'last_refresh'")
            result = cursor.fetchone()
            last_sync = result[0] if result else "Never"
            st.metric("Last Database Sync", last_sync)
            
            # Average CTR
            cursor.execute("SELECT AVG(ctr) FROM ads WHERE ctr IS NOT NULL")
            avg_ctr = cursor.fetchone()[0] or 0
            st.metric("Average CTR", f"{avg_ctr:.2%}")

def load_performance_data():
    """Load performance data from the database and convert to pandas DataFrame."""
    loader = FacebookAdsLoader('facebook_ads.db')
    metrics = loader.get_performance_metrics()
    
    if not metrics:
        return None
        
    # Convert to DataFrame
    df = pd.DataFrame(metrics)
    # Convert timestamp to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    # Sort by timestamp
    df = df.sort_values('timestamp')
    return df

def display_performance_metrics(performance_data: pd.DataFrame, ads_data: pd.DataFrame):
    """Display performance metrics with creative components and derived custom conversion counts."""
    try:
        # Initial checks
        if performance_data is None or performance_data.empty:
            st.warning("No performance data available. Please run the ETL process first.")
            return 
        if ads_data is None or ads_data.empty:
             st.warning("No ads data available. Please run the ETL process first.")
             return

        required_perf_cols = ['timestamp', 'ad_id', 'impressions', 'clicks', 'spend']
        optional_perf_cols = ['actions'] 
        missing_perf_cols = [col for col in required_perf_cols if col not in performance_data.columns]
        if missing_perf_cols:
            st.error(f"Performance data is missing required columns: {missing_perf_cols}")
            return
        has_actions_col = 'actions' in performance_data.columns

        required_ads_cols = ['id', 'creative_data']
        missing_ads_cols = [col for col in required_ads_cols if col not in ads_data.columns]
        if missing_ads_cols:
             st.error(f"Ads data is missing required columns: {missing_ads_cols}")
             return

        # Date range selector
        performance_data['date'] = pd.to_datetime(performance_data['timestamp']).dt.date
        min_date = performance_data['date'].min()
        max_date = performance_data['date'].max()
        date_range = st.date_input(
            "Select Date Range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date
        )
        
        # Filter data by date range
        if len(date_range) == 2:
            start_date, end_date = date_range
            if not isinstance(start_date, pd.Timestamp):
                start_date = pd.to_datetime(start_date).date()
            if not isinstance(end_date, pd.Timestamp):
                end_date = pd.to_datetime(end_date).date()
            
            mask = (performance_data['date'] >= start_date) & (performance_data['date'] <= end_date)
            filtered_data = performance_data[mask]
        else:
            filtered_data = performance_data
        
        if filtered_data.empty:
            st.warning("No data available for the selected date range.")
            return

        # Aggregate metrics by ad
        agg_metrics = {
            'impressions': 'sum',
            'clicks': 'sum',
            'spend': 'sum',
        }
        # Add optional standard metrics if they exist
        if 'reach' in filtered_data.columns:
            agg_metrics['reach'] = 'sum'
        if 'conversions' in filtered_data.columns: # This might be from insights directly
            agg_metrics['conversions'] = 'sum' 
        if 'conversion_value' in filtered_data.columns: # This might be from insights directly
            agg_metrics['conversion_value'] = 'sum'

        # Process 'actions' data if available before aggregation
        if has_actions_col:
            # 1. Safely parse the 'actions' JSON string in each row
            filtered_data['parsed_actions'] = filtered_data['actions'].apply(safe_json_loads)
            # 2. Calculate custom conversion *count* for each row
            filtered_data['custom_conv_count_row'] = filtered_data['parsed_actions'].apply(process_custom_conversions_from_actions).fillna(0).astype(int)
            # 3. Add this new column to the aggregation metrics
            agg_metrics['custom_conv_count_row'] = 'sum'
            
            # Optional: Display warning if actions column exists but processing yields no count
            if filtered_data['custom_conv_count_row'].sum() == 0:
                st.info("Note: 'actions' column found, but no count derived from 'offsite_conversion.fb_pixel_custom' actions in the selected date range.")

        # Check if ALL aggregation keys exist before agg
        agg_keys_exist = all(key in filtered_data.columns for key in agg_metrics.keys())
        if not agg_keys_exist:
            missing_agg_keys = [key for key in agg_metrics.keys() if key not in filtered_data.columns]
            st.error(f"Filtered data is missing columns needed for aggregation: {missing_agg_keys}")
            return
        
        # Group by 'ad_id' which must exist
        if 'ad_id' not in filtered_data.columns:
             st.error("Filtered data is missing 'ad_id' column for grouping.")
             return

        ad_metrics = filtered_data.groupby('ad_id').agg(agg_metrics).reset_index()
        
        # Rename the aggregated custom conversion *count* column for clarity
        if 'custom_conv_count_row' in ad_metrics.columns:
            ad_metrics = ad_metrics.rename(columns={'custom_conv_count_row': 'custom_conv_count'})

        # Calculate derived metrics (check existence of required cols first)
        if 'clicks' in ad_metrics.columns and 'impressions' in ad_metrics.columns:
            ad_metrics['ctr'] = (
                ad_metrics['clicks'] / ad_metrics['impressions'].replace(0, pd.NA) * 100
            ).round(2)
        if 'spend' in ad_metrics.columns and 'clicks' in ad_metrics.columns:
            ad_metrics['cpc'] = (
                ad_metrics['spend'] / ad_metrics['clicks'].replace(0, pd.NA)
            ).round(2)
        if 'spend' in ad_metrics.columns and 'impressions' in ad_metrics.columns:
            ad_metrics['cpm'] = (
                ad_metrics['spend'] / ad_metrics['impressions'].replace(0, pd.NA) * 1000
            ).round(2)
        if 'conversions' in ad_metrics.columns and 'clicks' in ad_metrics.columns:
            ad_metrics['conversion_rate'] = (
                ad_metrics['conversions'] / ad_metrics['clicks'].replace(0, pd.NA) * 100
            ).round(2)
        
        # Merge with ads data
        # Check merge keys exist
        if 'ad_id' not in ad_metrics.columns:
             st.error("Ad metrics data is missing 'ad_id' column for merge.")
             return
        if 'id' not in ads_data.columns:
             st.error("Ads data is missing 'id' column for merge.")
             return

        merged_data = pd.merge(ad_metrics, ads_data, left_on='ad_id', right_on='id')
        
        # Sort options - build dynamically based on merged_data columns
        # Include the new custom conversion *count* in sort options if calculated
        sortable_metrics_base = ['spend', 'impressions', 'clicks', 'conversions', 'conversion_value', 'conversion_rate', 'custom_conv_count', 'ctr', 'cpc', 'cpm']
        # Removed custom_conv_value if it's only count we care about, or keep if both needed
        # Let's assume we only derived count now. If value is also needed, it must come from elsewhere or a different processing step.

        available_metrics = [col for col in sortable_metrics_base if col in merged_data.columns]
        
        if not available_metrics:
             st.warning("No sortable metrics found in the merged data.")
             sort_by = None # Cannot sort
             merged_data_sorted = merged_data # Proceed unsorted
        else:
            # Default sort to custom conv count if available, otherwise first metric
            default_sort = 'custom_conv_count' if 'custom_conv_count' in available_metrics else available_metrics[0]
            sort_by = st.selectbox("Sort by", available_metrics, index=available_metrics.index(default_sort))
            # Sort data
            merged_data_sorted = merged_data.sort_values(by=sort_by, ascending=False, na_position='last')

        # Display ads with metrics
        for index, row in merged_data_sorted.iterrows():
            with st.container():
                col1, col2 = st.columns([2, 1])
                
                with col1:
                    # Ad creative - use .get() for safety
                    creative_raw = row.get('creative_data', {}) 
                    # Ensure creative_raw is treated as dict, even if it's None/NaN from merge/db
                    creative = creative_raw if isinstance(creative_raw, dict) else {}
                    
                    if creative.get('image_url'):
                        st.image(creative['image_url'], width=300)
                    st.subheader(creative.get('headline', 'No headline'))
                    st.write(creative.get('text', 'No ad text'))
                    
                    # Metrics table - build dynamically
                    metric_map = {
                        'impressions': ('Impressions', '{:,.0f}'),
                        'clicks': ('Clicks', '{:,.0f}'),
                        'spend': ('Spend ($)', '{:,.2f}'),
                        'ctr': ('CTR (%)', '{:,.2f}%'),
                        'cpc': ('CPC ($)', '{:,.2f}'),
                        'cpm': ('CPM ($)', '{:,.2f}'),
                        'conversions': ('Conversions (Total)', '{:,.0f}'),
                        'conversion_rate': ('Conv. Rate (%)', '{:,.2f}%'),
                        'conversion_value': ('Conv. Value (Total) ($)', '{:,.2f}'),
                        'custom_conv_count': ('Custom Conv. Count', '{:,.0f}')
                    }
                    
                    metrics_list = []
                    for key, (label, fmt) in metric_map.items():
                        if key in row and pd.notna(row[key]):
                            try:
                                # Apply formatting, handle potential errors (e.g., non-numeric type)
                                value_to_format = row[key]
                                if isinstance(value_to_format, str): # Attempt conversion if string
                                    try:
                                        value_to_format = float(value_to_format)
                                    except ValueError:
                                        pass # Keep as string if conversion fails
                                        
                                if isinstance(value_to_format, (int, float)): 
                                    formatted_value = fmt.format(value_to_format)
                                else: 
                                     formatted_value = str(value_to_format)
                                     
                                metrics_list.append({'Metric': label, 'Value': formatted_value})
                            except (ValueError, TypeError, KeyError) as format_err:
                                st.warning(f"Could not format metric '{key}' ({row.get(key)}): {format_err}")
                                metrics_list.append({'Metric': label, 'Value': str(row.get(key, 'Error'))})
                        # Optionally handle NaN/None case explicitly if desired (e.g., show 'N/A')
                        # elif key in row:
                        #     metrics_list.append({'Metric': label, 'Value': 'N/A'})

                    if metrics_list:
                        metrics_df = pd.DataFrame(metrics_list)
                        # Use set_index for better table layout if Metric is unique
                        try:
                            st.table(metrics_df.set_index('Metric')) 
                        except ValueError: # Handle duplicate metric keys if they somehow occur
                            st.table(metrics_df)
                    else:
                        st.write("No displayable metrics available for this ad.")

                
                with col2:
                    # Trend line
                    if sort_by and 'ad_id' in row and pd.notna(row['ad_id']):
                        ad_id_to_filter = row['ad_id']
                        # Filter original performance data for the trend
                        ad_trend = filtered_data[filtered_data['ad_id'] == ad_id_to_filter].copy()
                        
                        # Convert date to datetime if it isn't already
                        ad_trend['date'] = pd.to_datetime(ad_trend['date'])
                        
                        # Create a complete date range
                        if not ad_trend.empty:
                            date_range = pd.date_range(
                                start=ad_trend['date'].min(),
                                end=ad_trend['date'].max(),
                                freq='D'
                            )
                            
                            # Create a template DataFrame with all dates
                            template_df = pd.DataFrame({'date': date_range})
                            
                            # Ensure ad_trend has data before attempting plots
                            try:
                                if sort_by == 'custom_conv_count':
                                    # Ensure we have the required columns
                                    if 'custom_conv_count_row' not in ad_trend.columns:
                                        st.write("No custom conversion data available for trend.")
                                        return
                                        
                                    # Group by date and sum the conversions
                                    daily_data = ad_trend.groupby('date', as_index=False)['custom_conv_count_row'].sum()
                                    
                                    # Merge with template to ensure all dates are present
                                    complete_data = template_df.merge(
                                        daily_data,
                                        on='date',
                                        how='left'
                                    ).fillna(0)
                                    
                                    # Sort by date
                                    complete_data = complete_data.sort_values('date')
                                    
                                    # Create the plot
                                    fig = go.Figure()
                                    fig.add_trace(
                                        go.Scatter(
                                            x=complete_data['date'],
                                            y=complete_data['custom_conv_count_row'],
                                            mode='lines+markers',
                                            name='Custom Conversions',
                                            line=dict(width=2),
                                            marker=dict(size=8)
                                        )
                                    )
                                    
                                    # Update layout
                                    fig.update_layout(
                                        title='Daily Custom Conversions',
                                        xaxis_title='Date',
                                        yaxis_title='Conversions',
                                        height=200,
                                        margin=dict(l=20, r=20, t=40, b=20),
                                        showlegend=False,
                                        xaxis=dict(
                                            tickformat='%Y-%m-%d',
                                            tickmode='auto',
                                            nticks=10
                                        ),
                                        yaxis=dict(
                                            tickmode='auto',
                                            nticks=5,
                                            rangemode='nonnegative'
                                        )
                                    )
                                    
                                    # Display the plot with unique key
                                    st.plotly_chart(fig, use_container_width=True, key=f"custom_conv_plot_{ad_id_to_filter}_{sort_by}")
                                    
                                elif sort_by in ad_trend.columns:
                                    # Group by date for the selected metric
                                    daily_data = ad_trend.groupby('date', as_index=False)[sort_by].sum()
                                    
                                    # Merge with template to ensure all dates are present
                                    complete_data = template_df.merge(
                                        daily_data,
                                        on='date',
                                        how='left'
                                    ).fillna(0)
                                    
                                    # Sort by date
                                    complete_data = complete_data.sort_values('date')
                                    
                                    # Create the plot
                                    fig = go.Figure()
                                    fig.add_trace(
                                        go.Scatter(
                                            x=complete_data['date'],
                                            y=complete_data[sort_by],
                                            mode='lines+markers',
                                            name=sort_by.replace('_', ' ').title(),
                                            line=dict(width=2),
                                            marker=dict(size=8)
                                        )
                                    )
                                    
                                    # Update layout
                                    fig.update_layout(
                                        title=f'{sort_by.replace("_", " ").title()} Over Time',
                                        xaxis_title='Date',
                                        yaxis_title=sort_by.replace('_', ' ').title(),
                                        height=200,
                                        margin=dict(l=20, r=20, t=40, b=20),
                                        showlegend=False,
                                        xaxis=dict(
                                            tickformat='%Y-%m-%d',
                                            tickmode='auto',
                                            nticks=10
                                        ),
                                        yaxis=dict(
                                            tickmode='auto',
                                            nticks=5,
                                            rangemode='nonnegative'
                                        )
                                    )
                                    
                                    # Display the plot with unique key
                                    st.plotly_chart(fig, use_container_width=True, key=f"metric_plot_{ad_id_to_filter}_{sort_by}")
                                else:
                                    st.write(f"Cannot create trend for '{sort_by}'. Metric not found in data.")
                            except Exception as plot_error:
                                st.write(f"Error creating trend plot: {str(plot_error)}")
                        else:
                            st.write("No trend data available for this ad in the selected range.")
                
                st.divider()
        
    except Exception as e:
        st.error(f"Error displaying performance metrics: {str(e)}")
        st.error(f"Traceback:\n{traceback.format_exc()}") # Keep traceback for dev
        # raise # Re-raising might stop execution of other tabs

def get_table_schema(db_path: str) -> dict:
    """Get the schema of all tables in the database."""
    schema = {}
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        
        # Get list of tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        for table in tables:
            table_name = table[0]
            # Get table info
            cursor.execute(f"PRAGMA table_info({table_name});")
            columns = cursor.fetchall()
            
            # Get foreign keys
            cursor.execute(f"PRAGMA foreign_key_list({table_name});")
            foreign_keys = cursor.fetchall()
            
            schema[table_name] = {
                'columns': columns,  # (cid, name, type, notnull, dflt_value, pk)
                'foreign_keys': foreign_keys  # (id, seq, table, from, to, on_update, on_delete, match)
            }
    
    return schema

def create_schema_diagram():
    """Create and display database schema diagram."""
    st.header("Database Schema")
    
    # Get current schema
    schema = get_table_schema('facebook_ads.db')
    
    # Create graphviz diagram
    dot = Digraph(comment='Facebook Ads Database Schema')
    dot.attr(rankdir='LR')
    
    # Color scheme
    table_color = '#E6F3FF'
    pk_color = '#FFE6E6'
    fk_color = '#E6FFE6'
    
    # Add tables
    for table_name, table_info in schema.items():
        # Create HTML-like label for table
        table_label = f'''<<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4">
            <TR><TD PORT="header" BGCOLOR="#4A90E2" COLSPAN="3"><FONT COLOR="white"><B>{table_name}</B></FONT></TD></TR>
            <TR><TD><B>Column</B></TD><TD><B>Type</B></TD><TD><B>Constraints</B></TD></TR>'''
        
        # Add columns
        for col in table_info['columns']:
            col_id, col_name, col_type, notnull, dflt_value, is_pk = col
            
            # Build constraints string
            constraints = []
            if is_pk:
                constraints.append('PK')
            if notnull:
                constraints.append('NOT NULL')
            if dflt_value is not None:
                constraints.append(f'DEFAULT {dflt_value}')
            
            # Check if column is a foreign key
            is_fk = any(fk[3] == col_name for fk in table_info['foreign_keys'])
            if is_fk:
                constraints.append('FK')
            
            # Determine background color
            bg_color = pk_color if is_pk else (fk_color if is_fk else table_color)
            
            table_label += f'''<TR><TD PORT="{col_name}" BGCOLOR="{bg_color}">{col_name}</TD>
                <TD BGCOLOR="{bg_color}">{col_type}</TD>
                <TD BGCOLOR="{bg_color}">{', '.join(constraints)}</TD></TR>'''
        
        table_label += '</TABLE>>'
        
        # Add table node
        dot.node(table_name, label=table_label, shape='none')
    
    # Add relationships
    for table_name, table_info in schema.items():
        for fk in table_info['foreign_keys']:
            from_col = fk[3]
            to_table = fk[2]
            to_col = fk[4]
            
            dot.edge(f'{table_name}:{from_col}', f'{to_table}:{to_col}',
                    arrowhead='crow',
                    arrowtail='none',
                    dir='both')
    
    # Render diagram
    st.graphviz_chart(dot)
    
    # Add schema documentation
    with st.expander("Schema Documentation"):
        st.markdown("""
        ### Tables Description
        
        #### ads
        The main table storing Facebook ad information:
        - Basic ad details (ID, name, status)
        - Campaign and ad set relationships
        - Timestamps for creation and updates
        - Creative data in JSON format
        
        #### performance_metrics
        Time-series performance data for each ad:
        - Daily metrics (impressions, clicks, spend)
        - Calculated metrics (CTR, CPC, CPM)
        - Reach and frequency data
        - Action data in JSON format
        
        ### Relationships
        - Each performance metric record is linked to an ad via `ad_id` foreign key
        - The `timestamp` field in performance_metrics enables time-series analysis
        - Indexed fields optimize query performance
        
        ### Data Types
        - Text fields: Store string data (names, IDs, statuses)
        - INTEGER: Store whole numbers (impressions, clicks)
        - REAL: Store decimal numbers (spend, CTR, CPC)
        - JSON: Stored as TEXT but contains structured data (creative_data, actions)
        """)

def safe_json_loads(json_str, default_val=None):
    if default_val is None:
        default_val = [] # Default to empty list if JSON is expected to be a list
    if not isinstance(json_str, str):
        # If it's not a string, it might already be parsed or is None/NaN
        # Return it directly if it looks like a list/dict, otherwise default
        return json_str if isinstance(json_str, (list, dict)) else default_val
    try:
        return json.loads(json_str)
    except json.JSONDecodeError:
        return default_val

def process_custom_conversions_from_actions(actions_data):
    """
    Extract custom conversion count from actions data (list of dicts).
    Only counts offsite_conversion.fb_pixel_custom actions with 28d_click values.
    """
    print(f"\n=== Processing Conversions ===")
    print(f"Input actions_data type: {type(actions_data)}")
    print(f"Raw actions_data: {json.dumps(actions_data, indent=2)}")
    
    if not isinstance(actions_data, list):
        print("Error: actions_data is not a list")
        return 0
    
    # Find the highest 28d_click value for offsite_conversion.fb_pixel_custom
    highest_28d_click = 0
    
    print("\nProcessing each action:")
    for idx, action in enumerate(actions_data):
        print(f"\nAction {idx + 1}:")
        if not isinstance(action, dict):
            print(f"Skipping: Not a dictionary - {type(action)}")
            continue
            
        action_type = action.get('action_type')
        print(f"Action type: {action_type}")
        
        if action_type != 'offsite_conversion.fb_pixel_custom':
            print(f"Skipping: Not fb_pixel_custom")
            continue
            
        print(f"Found fb_pixel_custom action")
        print(f"Full action data: {json.dumps(action, indent=2)}")
        
        # Only use 28d_click as source of truth
        if '28d_click' in action:
            value = int(float(action['28d_click']))
            print(f"Found 28d_click value: {value}")
            if value > highest_28d_click:
                highest_28d_click = value
                print(f"New highest 28d_click value: {highest_28d_click}")
    
    print(f"\n=== Final Results ===")
    print(f"Highest 28d_click value: {highest_28d_click}")
    
    return highest_28d_click

def main():
    st.set_page_config(
        page_title="Facebook Ads Dashboard",
        page_icon="📊",
        layout="wide"
    )
    
    # Sidebar for ETL Controls
    with st.sidebar:
        st.title("Controls")
        st.header("ETL Process")

        # Date Range Selection for ETL
        st.subheader("Date Range")
        today = datetime.now().date()
        thirty_days_ago = today - timedelta(days=30)
        start_date_input = st.date_input("Start Date", value=thirty_days_ago, key="etl_start_date")
        end_date_input = st.date_input("End Date", value=today, key="etl_end_date")

        run_etl_button = st.button("Run Full ETL Process")

        if run_etl_button:
            # Validate dates
            if start_date_input > end_date_input:
                st.sidebar.error("Error: Start date must be before or equal to end date.")
            else:
                with st.spinner(f"Running ETL Process for {start_date_input} to {end_date_input}..."):
                    try:
                        # Dynamically import and run to avoid issues if main_etl is the entry point
                        import importlib
                        main_etl_module = importlib.import_module("main_etl")
                        # Pass selected dates to the run_etl function
                        main_etl_module.run_etl(start_date=start_date_input, end_date=end_date_input)
                        st.sidebar.success("ETL Process Completed!")
                        st.rerun()  # Use st.rerun() instead of experimental_rerun
                    except ImportError:
                        st.sidebar.error("Error: main_etl.py or run_etl function not found.")
                    except TypeError as te:
                        # Specific error if run_etl doesn't accept dates yet
                        if "run_etl() got an unexpected keyword argument" in str(te):
                             st.sidebar.error("ETL Error: The run_etl function needs to be updated to accept start_date and end_date.")
                             st.error(f"Please update main_etl.py: {te}")
                        else:
                            st.sidebar.error(f"ETL Error: {te}")
                            st.error(f"ETL Process failed: {te}")
                            st.error(f"Traceback:\n{traceback.format_exc()}")
                    except Exception as e:
                        st.sidebar.error(f"ETL failed: {e}")
                        st.error(f"ETL Process failed: {e}")
                        st.error(f"Traceback:\n{traceback.format_exc()}")

    # Main content area with tabs
    st.header("Facebook Ads Analytics")
    
    tab1, tab_schema = st.tabs(["Performance Analytics", "Database Schema"])

    with tab1:
        try:
            loader = FacebookAdsLoader('facebook_ads.db')
            performance_data = pd.DataFrame(loader.get_performance_metrics())
            ads_data = pd.DataFrame(loader.get_ads())

            if not performance_data.empty and not ads_data.empty:
                 # Add title within the tab for clarity
                 st.subheader("Ad Performance Breakdown") 
                 display_performance_metrics(performance_data, ads_data)
            elif performance_data.empty:
                 st.warning("No performance data loaded. Run ETL from sidebar?") 
            elif ads_data.empty:
                 st.warning("No ads data loaded. Run ETL from sidebar?") 
                 
        except Exception as e:
            st.error(f"Error loading data for Performance Analytics: {str(e)}")
            st.error(f"Traceback:\n{traceback.format_exc()}")
    
    with tab_schema:
        st.subheader("Database Schema")
        try:
            db_schema = get_table_schema('facebook_ads.db')
            if db_schema:
                # Display schema using st.expander for each table for better readability
                for table_name, table_info in db_schema.items():
                    with st.expander(f"Table: {table_name}"):
                        column_data = table_info['columns']
                        # Define column names based on PRAGMA table_info output
                        df_columns = pd.DataFrame(column_data, columns=['cid', 'name', 'type', 'notnull', 'dflt_value', 'pk'])
                        st.dataframe(df_columns.set_index('cid'))
            else:
                st.warning("Could not retrieve database schema or database is empty.")
        except Exception as e:
            st.error(f"Error retrieving database schema: {e}")
            st.error(f"Traceback:\n{traceback.format_exc()}")
            
if __name__ == "__main__":
    main() 