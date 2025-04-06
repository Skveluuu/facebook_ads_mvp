"""
Main ETL script orchestration.
"""
import traceback
from datetime import date, timedelta
import logging
import os
import sys

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Update imports to use absolute paths
from src.etl.extractors import FacebookAdsExtractor
from src.etl.transformers import FacebookAdsTransformer
from src.etl.loaders import FacebookAdsLoader
from src.facebook_api import FacebookAPI

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_etl(start_date: date | None = None, end_date: date | None = None):
    """
    Runs the main ETL process for Facebook Ads.

    Args:
        start_date: Optional start date for the data extraction range.
        end_date: Optional end date for the data extraction range.

    Returns:
        A dictionary indicating the status ('success' or 'error')
        and relevant messages or error details.
    """
    logger.info("Starting ETL process...")
    if start_date and end_date:
        logger.info(f"Date range specified: {start_date} to {end_date}")
    else:
        logger.info("No date range specified, fetching default range (last 30 days).")

    status = {'status': 'success', 'message': '', 'error': None}

    try:
        # 1. Initialize components
        logger.info("Initializing ETL components...")
        extractor = FacebookAdsExtractor()
        transformer = FacebookAdsTransformer()
        loader = FacebookAdsLoader('facebook_ads.db')
        api = FacebookAPI(
            access_token=os.getenv('FACEBOOK_ACCESS_TOKEN'),
            ad_account_id=os.getenv('FACEBOOK_AD_ACCOUNT_ID'),
            api_version=os.getenv('FACEBOOK_API_VERSION', 'v19.0')
        )
        logger.info("Components initialized.")

        # 2. Extract and Load Pixel Data (if applicable)
        pixel_id = None
        try:
            pixel_id = extractor.get_pixel_id_from_account()
            if pixel_id:
                logger.info(f"Found Facebook Pixel (ID: {pixel_id})")
                pixel_data = extractor.extract_pixel_events(pixel_id)
                if pixel_data:
                    loader.load_pixel_data(pixel_data)
                    logger.info(f"Successfully fetched and loaded pixel data for {pixel_id}.")
                else:
                    logger.info(f"No event data found for pixel {pixel_id}.")
            else:
                logger.info("No primary Facebook Pixel found associated with the account.")
        except Exception as pixel_err:
            logger.warning(f"Could not process pixel data - {pixel_err}")

        # 3. Extract and Load Custom Conversion Data (if applicable)
        try:
            conversions = extractor.list_custom_conversions()
            if conversions:
                logger.info(f"Found {len(conversions)} custom conversions. Fetching data for all...")
                for conv_meta in conversions:
                    try:
                        conversion_data = extractor.extract_custom_conversion_data(conv_meta['id'])
                        if conversion_data:
                            loader.load_custom_conversion(conversion_data)
                            logger.info(f"Successfully fetched and loaded data for conversion: {conversion_data['name']} ({conversion_data['id']})")
                            
                            # Check if this conversion uses a different pixel and load its data too
                            conv_pixel_id = conversion_data.get('pixel_id')
                            if conv_pixel_id and conv_pixel_id != pixel_id:
                                logger.info(f"Conversion {conversion_data['name']} uses pixel {conv_pixel_id}. Fetching its data...")
                                try:
                                    conv_pixel_data = extractor.extract_pixel_events(conv_pixel_id)
                                    if conv_pixel_data:
                                        loader.load_pixel_data(conv_pixel_data)
                                        logger.info(f"Successfully loaded data for conversion pixel {conv_pixel_id}.")
                                    else:
                                        logger.info(f"No event data found for conversion pixel {conv_pixel_id}.")
                                except Exception as conv_pixel_err:
                                    logger.warning(f"Could not process data for conversion pixel {conv_pixel_id} - {conv_pixel_err}")
                    except Exception as single_conv_err:
                        logger.warning(f"Failed to process conversion {conv_meta.get('name', conv_meta.get('id'))} - {single_conv_err}")
            else:
                logger.info("No custom conversions found in the account.")
        except Exception as conv_err:
            logger.warning(f"Could not process custom conversions - {conv_err}")

        # 4. Extract Ads and Performance Data using original method
        logger.info("Starting ads and performance data extraction...")
        try:
            # Use the original method to fetch offsite conversions
            ads_data, performance_data, extraction_status = extractor.extract_ads_async(
                start_date=start_date,
                end_date=end_date
            )
            
            if not ads_data:
                logger.warning("No conversion data extracted for the specified period.")
                status['message'] = "No conversion data found to process for the period."
            else:
                logger.info(f"Extracted {len(ads_data)} ad records.")
                
                # Transform and load the data
                logger.info("Starting data transformation...")
                transformed_data = transformer.transform_ads_batch(ads_data)
                logger.info(f"Transformed {len(transformed_data)} records.")
                
                logger.info("Starting data loading...")
                loader.load_ads_data(transformed_data, performance_data)
                logger.info(f"Loaded {len(transformed_data)} records.")
                
                # Save to offsite_conversions.json
                if performance_data:
                    import json
                    os.makedirs('output', exist_ok=True)
                    with open('output/offsite_conversions.json', 'w') as f:
                        json.dump(performance_data, f, indent=2)
                    logger.info("Data saved to output/offsite_conversions.json")
                
                # Update final status message
                status['message'] = f"Successfully processed {len(transformed_data)} records."
                logger.info("ETL process completed successfully.")

        except Exception as e:
            logger.error(f"Error during data extraction: {str(e)}")
            status['status'] = 'error'
            status['message'] = f"Data extraction failed: {str(e)}"
            status['error'] = traceback.format_exc()
            return status

    except Exception as e:
        logger.error(f"ETL Error: {str(e)}")
        logger.error(f"Traceback:\n{traceback.format_exc()}")
        status['status'] = 'error'
        status['message'] = f"ETL process failed: {str(e)}"
        status['error'] = traceback.format_exc()

    return status

# Example of how to run it (optional, for testing)
if __name__ == "__main__":
    logger.info("Running ETL directly for testing...")
    # Example: Run for the last 7 days
    today = date.today()
    seven_days_ago = today - timedelta(days=7)
    result = run_etl(start_date=seven_days_ago, end_date=today)
    logger.info(f"ETL Run Result:\n{result}") 