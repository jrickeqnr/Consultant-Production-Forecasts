import requests
import pandas as pd
from datetime import datetime
import os
import json
import logging
import urllib3
import time
from typing import Dict, Any, List
from pyspark.dbutils import DBUtils  # Add Databricks secrets support

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Disable SSL warnings globally
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logger.warning("SSL certificate verification is disabled. Use with caution.")

# Initialize DBUtils for Databricks environment
try:
    dbutils = DBUtils()
except Exception as e:
    dbutils = None
    logger.warning(f"Failed to initialize DBUtils. This is expected in non-Databricks environments: {str(e)}")

# Dictionary of locations
LOCATION_MAPPING = {
    '1064': 'Alaska (incl field condensate)',
    '1070': 'Total US (incl field condensate)',
    '1235': 'Hawaii (incl field condensate)',
    '6460': 'Midland Texas',
    '6461': 'Delaware NM Basin',
    '6462': 'Total Permian Basin',
    '6463': 'Eagle Ford Basin',
    '6464': 'Williston',
    '6465': 'DJ Basin',
    '6466': 'Gulf of Mexico',
    '6468': 'Delaware TX Basin',
    '6495': 'Total US',
    '6496': 'PADD 1',
    '6497': 'PADD 2',
    '6498': 'PADD 3',
    '6499': 'PADD 4',
    '6500': 'PADD 5',
    '15149': 'Anadarko Basin',
    '15150': 'Outside Major Basins',
    '141778': 'Total US Offshore',
    '141840': 'Total US Onshore',
    '142073': 'Appalachia Basin',
    '142077': 'Arkoma Woodford Basin',
    '142078': 'Bakken MT Basin',
    '142079': 'Bakken ND Basin',
    '142086': 'Barnett Basin',
    '142093': 'Fayetteville Basin',
    '142097': 'Greater Green River Basin',
    '142101': 'Haynesville Basin',
    '142102': 'Powder River Basin',
    '142106': 'Niobrara Basin',
    '142110': 'Permian Basin Other',
    '142117': 'San Juan Basin',
    '142121': 'Uinta Basin',
    '142122': 'California'
}


class EnergyAspectsAPI:
    """Client for Energy Aspects API with rate limiting and retry capabilities"""
    
    def __init__(self, environment: str = 'prod', config_path: str = 'config.json'):
        """
        Initialize the API client
        
        Args:
            environment: Environment to use ('prod' or 'dev')
            config_path: Path to config file (used as fallback if not in Databricks)
        """
        # Try to get API key from Databricks secrets first
        try:
            if dbutils:
                self.api_key = dbutils.secrets.get(scope=f"{environment}_API_Scope", key="energy-aspects-api-key")
                logger.info("Successfully loaded API key from Databricks secrets")
            else:
                # Fall back to config file if not in Databricks environment
                with open(config_path, 'r') as f:
                    config = json.load(f)
                    self.api_key = config['ea']['api_key']
                logger.info("Loaded API key from config file")
        except Exception as e:
            logger.error(f"Failed to load API key: {str(e)}")
            raise
            
        self.base_url = "https://api.energyaspects.com/data"
        
        # Rate limiting parameters
        self.rate_limit_max = 5  # Maximum requests per window
        self.rate_limit_window = 10  # Window period in seconds
        self.request_timestamps: List[float] = []  # Timestamps of recent requests
        
        # HTTP session for connection pooling
        self.session = requests.Session()
        self.session.headers.update({"accept": "application/json"})
        self.session.verify = False  # Disable SSL verification
    
    def _manage_rate_limit(self):
        """
        Manage API rate limiting to avoid 429 errors
        
        Implements a sliding window rate limiter
        """
        current_time = time.time()
        
        # Remove timestamps older than the window
        self.request_timestamps = [t for t in self.request_timestamps 
                                  if current_time - t < self.rate_limit_window]
        
        # Check if we're at the rate limit
        if len(self.request_timestamps) >= self.rate_limit_max:
            # Calculate the time to wait until we can make another request
            oldest_timestamp = min(self.request_timestamps)
            wait_time = self.rate_limit_window - (current_time - oldest_timestamp) + 1
            
            if wait_time > 0:
                logger.info(f"Rate limit reached. Waiting {wait_time:.1f} seconds before next request.")
                time.sleep(wait_time)
                # Recursive call after waiting to ensure we're under the limit
                return self._manage_rate_limit()
        
        # We're under the rate limit, add the current timestamp
        self.request_timestamps.append(time.time())
    
    def make_api_request(self, endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Make a request to the API with rate limiting and retry logic
        
        Args:
            endpoint: API endpoint path (relative to base URL)
            params: Request parameters
            
        Returns:
            JSON response as dictionary
        """
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # Manage rate limiting before making the request
                self._manage_rate_limit()
                
                # Ensure endpoint has trailing slash
                if not endpoint.endswith('/'):
                    endpoint = f"{endpoint}/"
                    
                url = f"{self.base_url}/{endpoint}"
                
                # Make the API request with our session
                response = self.session.get(url, params=params)
                
                # Log the response status for debugging in verbose mode
                logger.debug(f"Response status: {response.status_code}")
                
                response.raise_for_status()
                
                # If we reach here, the request was successful
                try:
                    return response.json()
                except json.JSONDecodeError:
                    logger.warning(f"Failed to parse JSON response: {response.text[:200]}...")
                    return {}
                    
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:  # Too Many Requests
                    # Exponential backoff with longer waits for rate limiting
                    wait_time = (2 ** retry_count) * 10  # 10, 20, 40 seconds
                    logger.warning(f"Rate limit exceeded. Waiting {wait_time} seconds before retry.")
                    time.sleep(wait_time)
                    retry_count += 1
                else:
                    logger.warning(f"API request failed with status {e.response.status_code}: {e}. Retry {retry_count + 1}/{max_retries}")
                    retry_count += 1
                    time.sleep(5 * (retry_count + 1))  # Linear backoff
            except (requests.exceptions.RequestException, requests.exceptions.SSLError) as e:
                logger.warning(f"API request failed: {e}. Retry {retry_count + 1}/{max_retries}")
                retry_count += 1
                time.sleep(5 * (retry_count + 1))  # Linear backoff
        
        logger.error(f"Failed to make API request after {max_retries} attempts")
        return {}

    def get_monthly_release_dates(self, dataset_id: str, start_date: str) -> Dict[str, str]:
        """
        Get one release date per month for a dataset starting from start_date
        
        Args:
            dataset_id: Dataset ID to get release dates for
            start_date: Start date in YYYY-MM-DD format
            
        Returns:
            Dict mapping month (YYYY-MM) to the last release date in that month
        """
        # Get all release dates for this dataset
        all_release_dates = self._get_all_release_dates(dataset_id, start_date)
        
        if not all_release_dates:
            return {}
            
        # Group release dates by month and get the latest one per month
        monthly_releases = {}
        
        for date_str in all_release_dates:
            # Extract the date part and convert to datetime
            date_only = date_str.split('T')[0]
            date_dt = datetime.strptime(date_only, '%Y-%m-%d')
            
            # Create month key in format YYYY-MM
            month_key = date_dt.strftime('%Y-%m')
            
            # If this month doesn't exist yet or this date is later than existing one
            if (month_key not in monthly_releases or 
                date_str > monthly_releases[month_key]):
                monthly_releases[month_key] = date_str
        
        logger.info(f"Selected {len(monthly_releases)} monthly release dates out of {len(all_release_dates)} total for dataset {dataset_id}")
        return monthly_releases
    
    def _get_all_release_dates(self, dataset_id: str, start_date: str) -> List[str]:
        """
        Get all available release dates for a dataset
        
        Args:
            dataset_id: Dataset ID to get release dates for
            start_date: Start date in YYYY-MM-DD format
            
        Returns:
            List of release dates in their original format (including time component)
        """
        try:
            # Build query parameters
            params = {
                'api_key': self.api_key,
                'dataset_id': dataset_id
            }
            
            # Make API request to get dataset metadata including release dates
            response = self.make_api_request("datasets/timeseries", params)
            
            # The response is an array, not an object
            if not isinstance(response, list) or not response:
                logger.warning(f"Unexpected API response format for dataset ID {dataset_id}")
                return []
            
            # Get the first item in the array
            data = response[0]
            
            # Check if metadata and release_dates exist in the response
            if ('metadata' in data and 
                'release_dates' in data['metadata'] and 
                data['metadata']['release_dates']):
                
                # Extract dates (keep full ISO format with time)
                release_dates = []
                start_dt = datetime.strptime(start_date, '%Y-%m-%d')
                
                for date_str in data['metadata']['release_dates']:
                    # Get date component for comparison only
                    date_only = date_str.split('T')[0]
                    date_dt = datetime.strptime(date_only, '%Y-%m-%d')
                    
                    # Only include dates after the start date
                    if date_dt >= start_dt:
                        # Add the original date string with time component
                        release_dates.append(date_str)
                
                # Sort the dates chronologically
                release_dates.sort()
                
                if release_dates:
                    logger.info(f"Retrieved {len(release_dates)} release dates for dataset ID {dataset_id}")
                    return release_dates
                else:
                    logger.warning(f"No release dates found after {start_date} for dataset ID {dataset_id}")
                    return []
            else:
                logger.warning(f"No release_dates field found in metadata for dataset ID {dataset_id}")
                return []
        except Exception as e:
            logger.error(f"Error retrieving release dates for dataset ID {dataset_id}: {e}")
            return []

    def get_timeseries_data(self, dataset_id: str, release_date: str) -> Dict[str, float]:
        """
        Get timeseries data for a specific dataset and release date
        
        Args:
            dataset_id: Dataset ID to get data for
            release_date: Release date in format from API
            
        Returns:
            Dictionary mapping date strings to values
        """
        try:
            # Convert release date to datetime for manipulation
            # Extract just the date part if there's a time component
            release_date_str = release_date.split('T')[0] if 'T' in release_date else release_date
            release_dt = datetime.strptime(release_date_str, '%Y-%m-%d')
            
            # Create date_from parameter to only fetch forecasts (data after the release)
            # Use the first day of the release month as the filter
            date_from = release_dt.strftime('%Y-%m-01')
            
            # Build query parameters
            params = {
                'api_key': self.api_key,
                'dataset_id': dataset_id,
                'release_date': release_date,
                'date_from': date_from  # Only get data from this date forward (forecasts)
            }
            
            logger.debug(f"Fetching forecasts for dataset {dataset_id} from {date_from} with release date {release_date}")
            
            # Make the API request
            response = self.make_api_request("timeseries", params)
            
            # The response is a LIST of objects, not a single object
            if isinstance(response, list) and len(response) > 0:
                # Get the first item in the list
                data_obj = response[0]
                
                # Check if we have data in the response
                if 'data' in data_obj and data_obj['data']:
                    # Only include dates after the release date (forecasts)
                    forecast_data = {}
                    for date_str, value in data_obj['data'].items():
                        data_date = datetime.strptime(date_str.split('T')[0] if 'T' in date_str else date_str, '%Y-%m-%d')
                        if data_date >= release_dt:
                            forecast_data[date_str] = value
                    
                    logger.info(f"Retrieved {len(forecast_data)} forecast data points for release date {release_date}")
                    return forecast_data
                else:
                    logger.info(f"No data in response for release date {release_date}")
                    return {}
            else:
                logger.info(f"Empty or invalid response for release date {release_date}")
                return {}
        except Exception as e:
            logger.error(f"Error getting timeseries data for dataset {dataset_id}, release date {release_date}: {e}")
            return {}


def fetch_monthly_forecasts(api_client: EnergyAspectsAPI, start_date: str, debug_mode: bool = False) -> pd.DataFrame:
    """
    Fetch monthly historical forecasts for all locations
    
    Args:
        api_client: Initialized API client
        start_date: Start date in YYYY-MM-DD format
        debug_mode: If True, only process a few datasets for debugging
        
    Returns:
        DataFrame containing forecasts with one entry per month per location
    """
    # Create rows list to store all data
    rows = []
    
    # Get all datasets
    all_datasets = list(LOCATION_MAPPING.items())
    
    # If debug mode, only process 3 datasets
    if debug_mode:
        logger.info(f"DEBUG MODE: Only processing 3 datasets out of {len(all_datasets)}")
        datasets_to_process = all_datasets[:3]  # Take just the first 3
    else:
        logger.info(f"Processing all {len(all_datasets)} datasets")
        datasets_to_process = all_datasets  # Process all datasets
    
    # For each location
    for dataset_id, location_name in datasets_to_process:
        logger.info(f"Processing {location_name} (dataset ID: {dataset_id})...")
        
        # Get one release date per month
        monthly_release_dates = api_client.get_monthly_release_dates(dataset_id, start_date)
        
        if not monthly_release_dates:
            logger.warning(f"No monthly release dates found for {location_name}. Skipping.")
            continue
        
        # For each monthly release date
        for month, release_date in monthly_release_dates.items():
            # Get the forecast data for this release date (data after the release date)
            forecast_data = api_client.get_timeseries_data(dataset_id, release_date)
            
            if not forecast_data:
                logger.warning(f"No forecast data for {location_name} on release date {release_date}. Skipping.")
                continue
                
            # Get release date as datetime for comparison
            release_date_dt = datetime.strptime(release_date.split('T')[0] if 'T' in release_date else release_date, '%Y-%m-%d')
            
            # Process each data point in the forecast data
            for date_str, value in forecast_data.items():
                # Parse the data date for validation
                data_date = datetime.strptime(date_str.split('T')[0] if 'T' in date_str else date_str, '%Y-%m-%d')
                
                # Skip if this is historical data (before the release date)
                if data_date < release_date_dt:
                    logger.debug(f"Skipping historical data point {date_str} for release date {release_date}")
                    continue
                
                # Find existing row for this date and release date combination
                matching_row = None
                for row in rows:
                    if (row['Date'] == date_str and 
                        row['ReleaseMonth'] == month):
                        matching_row = row
                        break
                
                # If no existing row, create a new one
                if matching_row is None:
                    matching_row = {
                        'Date': date_str,
                        'ReleaseMonth': month,
                        'ReleaseDate': release_date
                    }
                    rows.append(matching_row)
                
                # Add this location's value to the row
                matching_row[location_name] = value
    
    # Create DataFrame from rows
    if rows:
        df = pd.DataFrame(rows)
        
        # Convert date columns to datetime
        df['Date'] = pd.to_datetime(df['Date'])
        
        # Extract just the date part from ReleaseDate for display
        df['ReleaseDate'] = pd.to_datetime(df['ReleaseDate']).dt.date
          # Convert dates to YYYY-MM format and rename columns
        df['month'] = df['Date'].dt.strftime('%Y-%m')
        df['reportDate'] = df['ReleaseMonth']
        
        # Reorder and drop columns
        first_cols = ['reportDate', 'month']
        other_cols = [col for col in df.columns if col not in first_cols + ['ReleaseMonth', 'ReleaseDate', 'Date']]
        df = df[first_cols + other_cols]
        
        # Sort by report date and then by data date
        df = df.sort_values(['reportDate', 'month'])
        return df
    else:
        logger.warning("No data was collected.")
        return pd.DataFrame()


def load_config(config_file="config.json"):
    """Load configuration from file or create with defaults if it doesn't exist"""
    if not os.path.exists(config_file):
        logger.error(f"Configuration file {config_file} not found. Please create it with your API key.")
        logger.info("Example config.json format: {\"ea\": {\"api_key\": \"your-api-key-here\", \"start_date\": \"YYYY-MM-DD\"}}")
        return None
    
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
            
        if "ea" not in config:
            logger.error("EA section not found in config file. Please add 'ea' section.")
            return None
            
        ea_config = config["ea"]
        if "api_key" not in ea_config:
            logger.error("API key not found in EA config section. Please add 'api_key' field.")
            return None
            
        return ea_config
    except json.JSONDecodeError:
        logger.error(f"Error parsing {config_file}. Please ensure it contains valid JSON.")
        return None
    except Exception as e:
        logger.error(f"Error loading config: {str(e)}")
        return None


def main():
    """Main execution function that returns a DataFrame of Energy Aspects forecasts"""
    try:
        # Load configuration
        config = load_config()
        
        if not config:
            return pd.DataFrame()
            
        # Get API key and start date from config
        api_key = config.get("api_key")
        start_date = config.get("start_date", "2019-12-01")  # Default to Dec 2019 if not specified
        debug_mode = config.get("debug", False)  # Optional debug mode

        logger.info(f"Fetching monthly crude production forecasts from Energy Aspects starting from {start_date}...")
        
        # Track start time for overall execution
        start_time = time.time()
        
        # Initialize API client
        api_client = EnergyAspectsAPI(api_key)
        
        # Fetch the forecasts with one report per month
        forecasts_df = fetch_monthly_forecasts(api_client, start_date, debug_mode)
        
        execution_time = time.time() - start_time
        
        if not forecasts_df.empty:
            logger.info(f"Data shape: {forecasts_df.shape}")
            logger.info(f"Total execution time: {execution_time:.2f} seconds")
            return forecasts_df
        else:
            logger.warning("No data to return.")
            return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        return pd.DataFrame()


if __name__ == "__main__":
    main()