"""
Main ETL script orchestration.
"""
import traceback
from datetime import date, timedelta

# Assuming your ETL classes are in these locations
from etl.extractors import FacebookAdsExtractor
from etl.transformers import FacebookAdsTransformer
from etl.loaders import FacebookAdsLoader

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
    print(f"Starting ETL process...")
    if start_date and end_date:
        print(f"Date range specified: {start_date} to {end_date}")
    else:
        print("No date range specified, fetching default range or all data.")

    status = {'status': 'success', 'message': '', 'error': None}

    try:
        # 1. Initialize components
        print("Initializing ETL components...")
        extractor = FacebookAdsExtractor()
        transformer = FacebookAdsTransformer()
        loader = FacebookAdsLoader('facebook_ads.db') # Assuming db name
        print("Components initialized.")

        # 2. Extract and Load Pixel Data (if applicable)
        pixel_id = None # Initialize pixel_id
        try:
            pixel_id = extractor.get_pixel_id_from_account()
            if pixel_id:
                print(f"Found Facebook Pixel (ID: {pixel_id})")
                pixel_data = extractor.extract_pixel_events(pixel_id)
                if pixel_data:
                    loader.load_pixel_data(pixel_data)
                    print(f"Successfully fetched and loaded pixel data for {pixel_id}.")
                else:
                    print(f"No event data found for pixel {pixel_id}.")
            else:
                print("No primary Facebook Pixel found associated with the account.")
        except Exception as pixel_err:
            print(f"Warning: Could not process pixel data - {pixel_err}")
            # Decide if this should be a warning or stop the ETL

        # 3. Extract and Load Custom Conversion Data (if applicable)
        try:
            conversions = extractor.list_custom_conversions()
            if conversions:
                print(f"Found {len(conversions)} custom conversions. Fetching data for all...")
                for conv_meta in conversions:
                    try:
                        conversion_data = extractor.extract_custom_conversion_data(conv_meta['id']) # Fetch by ID
                        if conversion_data:
                            loader.load_custom_conversion(conversion_data)
                            print(f"Successfully fetched and loaded data for conversion: {conversion_data['name']} ({conversion_data['id']})")
                            
                            # Check if this conversion uses a different pixel and load its data too
                            conv_pixel_id = conversion_data.get('pixel_id')
                            if conv_pixel_id and conv_pixel_id != pixel_id:
                                print(f"Conversion {conversion_data['name']} uses pixel {conv_pixel_id}. Fetching its data...")
                                try:
                                    conv_pixel_data = extractor.extract_pixel_events(conv_pixel_id)
                                    if conv_pixel_data:
                                        loader.load_pixel_data(conv_pixel_data)
                                        print(f"Successfully loaded data for conversion pixel {conv_pixel_id}.")
                                    else:
                                        print(f"No event data found for conversion pixel {conv_pixel_id}.")
                                except Exception as conv_pixel_err:
                                    print(f"Warning: Could not process data for conversion pixel {conv_pixel_id} - {conv_pixel_err}")
                    except Exception as single_conv_err:
                        print(f"Warning: Failed to process conversion {conv_meta.get('name', conv_meta.get('id'))} - {single_conv_err}")
            else:
                print("No custom conversions found in the account.")
        except Exception as conv_err:
            print(f"Warning: Could not process custom conversions - {conv_err}")
            # Decide if this should be a warning or stop the ETL

        # 4. Extract Ads and Performance Data (using date range)
        print("Starting ads and performance data extraction...")
        # *** This extractor call needs modification to use dates ***
        ads_data, performance_data, extract_status = extractor.extract_ads_async(start_date=start_date, end_date=end_date)

        print(f"Extraction status: {extract_status.get('status', 'Unknown')}")
        if extract_status.get('status') == 'ERROR':
            error_details = extract_status.get('response', 'Unknown error')
            if extract_status.get('errors'):
                 error_details += " Details: " + "; ".join(extract_status['errors'])
            raise Exception(f"Extraction failed: {error_details}")

        if not ads_data and not performance_data:
            print("No ads or performance data extracted for the specified period.")
            status['message'] = "No ads or performance data found to process for the period."
            # Depending on requirements, you might want to return success here
            # return status 
        else:
             print(f"Extracted {len(ads_data)} ads and {len(performance_data)} performance records.")

        # 5. Transform Ads Data
        transformed_ads = []
        if ads_data:
            print("Starting ads data transformation...")
            transformed_ads = transformer.transform_ads_batch(ads_data)
            print(f"Transformed {len(transformed_ads)} ads.")
        else:
             print("No ads data to transform.")

        # 6. Load Transformed Ads and Performance Data
        print("Starting data loading...")
        if transformed_ads:
            loader.load_ads(transformed_ads)
            print(f"Loaded {len(transformed_ads)} ads.")
        if performance_data:
            loader.load_performance_metrics(performance_data)
            print(f"Loaded {len(performance_data)} performance records.")
        print("Data loading complete.")

        # Update final status message
        processed_ads_count = len(transformed_ads) if transformed_ads else 0
        processed_perf_count = len(performance_data) if performance_data else 0
        status['message'] = f"Successfully processed {processed_ads_count} ads and {processed_perf_count} performance records."
        print("ETL process completed successfully.")

    except Exception as e:
        print(f"ETL Error: {str(e)}")
        print(f"Traceback:\n{traceback.format_exc()}")
        status['status'] = 'error'
        status['message'] = f"ETL process failed: {str(e)}"
        status['error'] = traceback.format_exc()

    return status

# Example of how to run it (optional, for testing)
if __name__ == "__main__":
    print("Running ETL directly for testing...")
    # Example: Run for the last 7 days
    today = date.today()
    seven_days_ago = today - timedelta(days=7)
    result = run_etl(start_date=seven_days_ago, end_date=today)
    print(f"ETL Run Result:\n{result}") 