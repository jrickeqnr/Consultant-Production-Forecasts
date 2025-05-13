import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import logging
import json
from typing import List, Dict, Any, Optional
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("oil_forecast_api.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("oil_forecast_api")

# Determine the correct output directory
# If running from "Genscape" subfolder, use parent directory for outputs
script_dir = os.path.dirname(os.path.abspath(__file__))
script_parent_dir = os.path.dirname(script_dir)

# Identify if we're running from a subfolder
running_from_subfolder = os.path.basename(script_dir) == "Genscape"

# Use parent directory for output if running from subfolder, otherwise use current directory
output_dir = script_parent_dir if running_from_subfolder else script_dir

# Configure logging with correct path
log_file_path = os.path.join(output_dir, "oil_forecast_api.log")

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("oil_forecast_api")

# Log the directory structure for debugging
logger.info(f"Script directory: {script_dir}")
logger.info(f"Parent directory: {script_parent_dir}")
logger.info(f"Running from subfolder: {running_from_subfolder}")
logger.info(f"Using output directory: {output_dir}")

class OilForecastAPI:
    """Client for accessing the Genscape Oil Production and Forecasting API"""
    
    BASE_URL = "https://api.genscape.com/oil-production-forecasting"
    ENDPOINT = "/v1/us-oil-production-forecast/monthly"
    
    def __init__(self, api_key: str):
        """
        Initialize the API client
        
        Args:
            api_key: API key for authentication
        """
        self.api_key = api_key
        self.headers = {
            "Accept": "application/json",
            "Gen-Api-Key": f"{api_key}"
        }
        # Rate limiting settings
        self.request_count = 0
        self.rate_limit_window = 60  # 60 seconds window
        self.rate_limit_max = 5      # Maximum requests per minute (reduced to be more conservative)
        self.request_timestamps = []
    
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
    
    def _make_api_request(self, report_date: Optional[str] = None, params: Dict[str, Any] = None, limit: int = 200) -> Dict[str, Any]:
        """
        Make a request to the API
        
        Args:
            report_date: Optional report date
            params: Additional parameters
            limit: Limit of records to retrieve
            
        Returns:
            JSON response as dictionary
        """
        url = f"{self.BASE_URL}{self.ENDPOINT}"
        request_params = params or {}
        
        if report_date:
            request_params["reportDate"] = report_date
            
        if "limit" not in request_params:
            request_params["limit"] = limit
        
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # Manage rate limiting before making the request
                self._manage_rate_limit()
                
                logger.debug(f"Making API request: {url} with params: {request_params}")
                response = requests.get(url, headers=self.headers, params=request_params)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:  # Too Many Requests
                    # Exponential backoff with longer waits for rate limiting
                    wait_time = (2 ** retry_count) * 10  # 10, 20, 40 seconds (increased)
                    logger.warning(f"Rate limit exceeded. Waiting {wait_time} seconds before retry.")
                    time.sleep(wait_time)
                    retry_count += 1
                else:
                    logger.warning(f"API request failed with status {e.response.status_code}: {e}. Retry {retry_count + 1}/{max_retries}")
                    retry_count += 1
                    time.sleep(5 * (retry_count + 1))  # Linear backoff, longer delays
            except requests.exceptions.RequestException as e:
                logger.warning(f"API request failed: {e}. Retry {retry_count + 1}/{max_retries}")
                retry_count += 1
                time.sleep(5 * (retry_count + 1))  # Linear backoff, longer delays
        
        logger.error(f"Failed to make API request after {max_retries} attempts")
        return {}
    
    def get_last_monday_of_month(self, year: int, month: int) -> str:
        """
        Get the last Monday of the specified month
        
        Args:
            year: Year (e.g., 2024)
            month: Month (1-12)
            
        Returns:
            Date string in YYYY-MM-DD format
        """
        # Get the first day of the next month
        if month == 12:
            next_month = datetime(year + 1, 1, 1)
        else:
            next_month = datetime(year, month + 1, 1)
        
        # Back up one day to get the last day of the current month
        last_day = next_month - timedelta(days=1)
        
        # Find the last Monday of this month
        days_before_monday = (last_day.weekday() - 0) % 7  # Monday is 0
        last_monday = last_day - timedelta(days=days_before_monday)
        
        # If the last Monday is in a different month, go back a week
        if last_monday.month != month:
            last_monday = last_monday - timedelta(days=7)
            
        return last_monday.strftime("%Y-%m-%d")
        
    def get_report_dates_by_month(self, start_date: str = "2019-12-01") -> List[str]:
        """
        Get the most recent report date from each month since the start date
        
        Args:
            start_date: Starting date in YYYY-MM-DD format
            
        Returns:
            List of report dates in YYYY-MM-DD format, one per month
        """
        # Convert start date to datetime
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        
        # Current date
        today = datetime.now()
        
        # Generate one report date per month (the last Monday of each month)
        report_dates = []
        current_year = start_dt.year
        current_month = start_dt.month
        
        while current_year < today.year or (current_year == today.year and current_month <= today.month):
            # For past months, use the last Monday of the month
            if current_year < today.year or current_month < today.month:
                report_date = self.get_last_monday_of_month(current_year, current_month)
                report_dates.append(report_date)
            else:  # For the current month, use the most recent Monday
                # Find the most recent Monday
                days_since_monday = today.weekday()
                if days_since_monday == 0:  # Today is Monday
                    most_recent = today
                else:
                    # Go back to the last Monday
                    most_recent = today - timedelta(days=days_since_monday)
                
                report_dates.append(most_recent.strftime("%Y-%m-%d"))
            
            # Move to the next month
            if current_month == 12:
                current_year += 1
                current_month = 1
            else:
                current_month += 1
        
        logger.info(f"Identified {len(report_dates)} monthly report dates (one per month) since {start_date}")
        return report_dates
        
    def get_report_dates(self, start_date: str = "2019-12-01") -> List[str]:
        """
        Get report dates (one per month) since the specified start date
        
        Args:
            start_date: Starting date in YYYY-MM-DD format
            
        Returns:
            List of report dates in YYYY-MM-DD format (one per month)
        """
        # Get one report date per month since the start date
        report_dates = self.get_report_dates_by_month(start_date)
        
        # Verify these dates with the API
        valid_dates = []
        
        for date in report_dates:
            # We only need to check if the date exists, so limit=1 is sufficient
            response = self._make_api_request(report_date=date, limit=1)
            
            if response and "data" in response and response["data"]:
                valid_dates.append(date)
                logger.info(f"Verified report date: {date}")
            else:
                logger.warning(f"Report date {date} is not valid, trying the previous Monday")
                # Try the previous Monday
                date_dt = datetime.strptime(date, "%Y-%m-%d")
                prev_monday = date_dt - timedelta(days=7)
                prev_date = prev_monday.strftime("%Y-%m-%d")
                
                response = self._make_api_request(report_date=prev_date, limit=1)
                if response and "data" in response and response["data"]:
                    valid_dates.append(prev_date)
                    logger.info(f"Using alternative report date: {prev_date}")
                else:
                    logger.warning(f"Could not find a valid report date for {date.split('-')[0]}-{date.split('-')[1]}")
            
            # Add a small delay between tests to avoid rate limiting
            time.sleep(2)
        
        logger.info(f"Found {len(valid_dates)} valid monthly report dates since {start_date}")
        return valid_dates
    
    def get_forecast_data(self, report_date: str) -> pd.DataFrame:
        """
        Get the oil production forecast data for a specific report date
        
        Args:
            report_date: The report date in YYYY-MM-DD format
            
        Returns:
            DataFrame containing the forecast data
        """
        # Convert the report_date string to datetime for proper API parameter
        report_date_dt = pd.to_datetime(report_date)
        min_month = report_date_dt.strftime("%Y-%m-%d")  # Use report date as the minimum month
        
        # Calculate max_month as 1 year from the report date
        max_month_dt = report_date_dt + pd.DateOffset(years=1)
        max_month = max_month_dt.strftime("%Y-%m-%d")
        
        params = {
            "items": "crudeOilProduction",
            "minMonth": min_month,  # Use the report date as minMonth to get only forecasts
            "maxMonth": max_month,  # Limit to 1 year of forecast data
            "limit": 5000  # Maximum limit for JSON responses
        }
        
        logger.info(f"Fetching forecast data for report date: {report_date} with minMonth: {min_month}, maxMonth: {max_month}")
        response = self._make_api_request(report_date=report_date, params=params)
        
        if not response or "data" not in response:
            logger.error(f"Failed to get forecast data for report date: {report_date}")
            return pd.DataFrame()
        
        # If data is empty list, return empty DataFrame
        if not response["data"]:
            logger.warning(f"API returned empty data for report date: {report_date}")
            return pd.DataFrame()
        
        # Convert to DataFrame
        df = pd.DataFrame(response["data"])
        logger.info(f"Retrieved {len(df)} rows of data for report date: {report_date}")
        
        # Format dates
        df["reportDate"] = pd.to_datetime(df["reportDate"])
        df["month"] = pd.to_datetime(df["month"])
        
        # Print date ranges to debug
        min_report_date = df["reportDate"].min() if not df.empty else None
        max_report_date = df["reportDate"].max() if not df.empty else None
        min_month = df["month"].min() if not df.empty else None
        max_month = df["month"].max() if not df.empty else None
        
        logger.info(f"Date ranges in data - Report date: {min_report_date} to {max_report_date}, " 
                   f"Month: {min_month} to {max_month}")
        
        # Double-check filtering at the application level (should be redundant with minMonth parameter)
        df_forecast = df[df["month"] >= df["reportDate"]]
        
        if len(df_forecast) < len(df):
            logger.warning(f"Additional filtering removed {len(df) - len(df_forecast)} rows despite minMonth parameter")
        
        logger.info(f"Final dataset has {len(df_forecast)} forecast rows")
        
        return df_forecast
    
    def get_all_forecasts_since(self, start_date: str = "2019-12-01") -> pd.DataFrame:
        """
        Get all forecasts since the specified date
        
        Args:
            start_date: Starting date in YYYY-MM-DD format
            
        Returns:
            DataFrame containing all forecast data
        """
        report_dates = self.get_report_dates(start_date)
        
        if not report_dates:
            logger.error(f"No valid report dates found since {start_date}")
            return pd.DataFrame()
        
        all_data = []
        for report_date in report_dates:
            df = self.get_forecast_data(report_date)
            
            if not df.empty:
                all_data.append(df)
                logger.info(f"Successfully added data for report date: {report_date}")
            else:
                logger.warning(f"No usable data for report date: {report_date}")
            
            # Add a longer delay between substantial data retrievals
            time.sleep(3)
        
        if not all_data:
            logger.error("No forecast data found for any report date")
            return pd.DataFrame()
        
        # Combine all data
        logger.info(f"Combining data from {len(all_data)} reports")
        combined_df = pd.concat(all_data, ignore_index=True)
        logger.info(f"Combined data has {len(combined_df)} rows")
        
        return combined_df
    
    def transform_to_wide_format(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform the data to the requested format with report date and month in MMM-YY format
        
        Args:
            df: DataFrame in API response format
            
        Returns:
            Transformed DataFrame in wide format
        """
        if df.empty:
            return df
        
        # Keep only necessary columns
        df = df[["reportDate", "month", "subRegion", "value"]]
        
        # Before formatting, save original dates for sorting
        df["reportDate_orig"] = df["reportDate"]
        df["month_orig"] = df["month"]
          # Convert reportDate to YYYY-MM format
        df["reportDate"] = df["reportDate"].dt.strftime("%Y-%m")
        
        # Convert month to YYYY-MM format as well
        df["month"] = df["month"].dt.strftime("%Y-%m")
        
        # Pivot to get subregions as columns
        try:
            wide_df = df.pivot_table(
                index=["reportDate", "reportDate_orig", "month", "month_orig"],
                columns="subRegion",
                values="value"
            ).reset_index()
            
            # Sort by original dates (before the formatting)
            wide_df = wide_df.sort_values(["reportDate_orig", "month_orig"])
            
            # Remove the original date columns used for sorting
            wide_df = wide_df.drop(columns=["reportDate_orig", "month_orig"])
            
            logger.info(f"Transformed data to wide format with {len(wide_df)} rows and {len(wide_df.columns)} columns")
            return wide_df
        except Exception as e:
            logger.error(f"Error transforming to wide format: {str(e)}")
            # Return original dataframe if transformation fails
            return df


def load_config(config_file="config.json"):
    """
    Load configuration from file with improved error handling and location checks
    
    Args:
        config_file: Name of the configuration file
        
    Returns:
        Dictionary containing configuration or None if file not found/invalid
    """
    # Paths to check for config file:
    # 1. Current working directory
    # 2. Script directory
    # 3. Parent directory (if running from subfolder)
    
    # Current working directory
    cwd_config_path = os.path.join(os.getcwd(), config_file)
    
    # Script directory
    script_config_path = os.path.join(script_dir, config_file)
    
    # Parent directory (if different)
    parent_config_path = os.path.join(script_parent_dir, config_file) if script_parent_dir != script_dir else None
    
    # List of paths to check
    paths_to_check = [path for path in [cwd_config_path, script_config_path, parent_config_path] if path is not None]
    
    logger.info(f"Checking for config file in: {paths_to_check}")
    
    for path in paths_to_check:
        if os.path.exists(path):
            logger.info(f"Found configuration file at: {path}")
            try:
                with open(path, 'r') as f:
                    config = json.load(f)
                
                if "genscape" not in config:
                    logger.error("Genscape section not found in config file. Please add 'genscape' section.")
                    return None
                    
                genscape_config = config["genscape"]
                if "api_key" not in genscape_config:
                    logger.error("API key not found in Genscape config section. Please add 'api_key' field.")
                    return None
                
                return genscape_config
            except json.JSONDecodeError:
                logger.error(f"Error parsing {path}. Please ensure it contains valid JSON.")
            except Exception as e:
                logger.error(f"Error loading config from {path}: {str(e)}")
    
    # If we get here, we didn't find a valid config file
    logger.error(f"Configuration file '{config_file}' not found in any of the checked directories.")
    logger.info("Please create a config.json file with your API key.")
    logger.info("Example config.json format: {\"genscape\": {\"api_key\": \"your-api-key-here\", \"start_date\": \"YYYY-MM-DD\"}}")
    return None
    
def add_calculated_regions(df):
    """
    Add calculated region columns based on combinations of existing columns
    
    Args:
        df: DataFrame in wide format with subregions as columns
        
    Returns:
        DataFrame with additional calculated region columns
    """
    # Make a copy to avoid modifying the original
    result_df = df.copy()
    
    # Create DJ Basin
    if "Wyoming Denver Julesberg" in result_df.columns and "Colorado Denver Julesberg" in result_df.columns:
        result_df["DJ Basin"] = result_df["Wyoming Denver Julesberg"] + result_df["Colorado Denver Julesberg"]
        logger.info("Added 'DJ Basin' column")
    else:
        logger.warning("Could not create 'DJ Basin' column: missing required source columns")
    
    # Create Permian Basin
    permian_regions = ["Permian New Mexico", "Texas Dist 7C", "Texas Dist 8", "Texas Dist 8A"]
    if all(region in result_df.columns for region in permian_regions):
        result_df["Permian Basin"] = result_df[permian_regions].sum(axis=1)
        logger.info("Added 'Permian Basin' column")
    else:
        logger.warning(f"Could not create 'Permian Basin' column: missing one or more required source columns: {permian_regions}")
    
    # Create Gulf of Mexico
    gulf_regions = ["Gulf of Mexico - Deepwater", "Gulf of Mexico - Shelf"]
    if all(region in result_df.columns for region in gulf_regions):
        result_df["Gulf of Mexico"] = result_df[gulf_regions].sum(axis=1)
        logger.info("Added 'Gulf of Mexico' column")
    else:
        logger.warning(f"Could not create 'Gulf of Mexico' column: missing one or more required source columns: {gulf_regions}")
    
    # Create Colorado
    colorado_regions = ["Colorado Denver Julesberg", "Colorado Other", "Colorado Piceance", "Colorado San Juan"]
    if all(region in result_df.columns for region in colorado_regions):
        result_df["Colorado"] = result_df[colorado_regions].sum(axis=1)
        logger.info("Added 'Colorado' column")
    else:
        logger.warning(f"Could not create 'Colorado' column: missing one or more required source columns: {colorado_regions}")
    
    # Create Utah
    utah_regions = ["Utah Other", "Utah Uinta"]
    if all(region in result_df.columns for region in utah_regions):
        result_df["Utah"] = result_df[utah_regions].sum(axis=1)
        logger.info("Added 'Utah' column")
    else:
        logger.warning(f"Could not create 'Utah' column: missing one or more required source columns: {utah_regions}")

    # Create Wyoming
    wyoming_regions = ["Wyoming Big Horn","Wyoming Green/Wind/OT","Wyoming Powder"]
    if all(region in result_df.columns for region in wyoming_regions):
        result_df["Utah"] = result_df[wyoming_regions].sum(axis=1)
        logger.info("Added 'Wyoming' column")
    else:
        logger.warning(f"Could not create 'Wyoming' column: missing one or more required source columns: {wyoming_regions}")

    # Create Texas
    texas_regions = ["Texas Dist 1","Texas Dist 10","Texas Dist 2","Texas Dist 3","Texas Dist 4","Texas Dist 5","Texas Dist 6","Texas Dist 7B","Texas Dist 7C","Texas Dist 8","Texas Dist 8A","Texas Dist 9"]
    if all(region in result_df.columns for region in texas_regions):
        result_df["Texas"] = result_df[texas_regions].sum(axis=1)
        logger.info("Added 'Texas' column")
    else:
        logger.warning(f"Could not create 'Texas' column: missing one or more required source columns: {texas_regions}")

    # Create New Mexico
    newMexico_regions = ["Permian New Mexico","New Mexico San Juan"]
    if all(region in result_df.columns for region in newMexico_regions):
        result_df["New Mexico"] = result_df[newMexico_regions].sum(axis=1)
        logger.info("Added 'New Mexico' column")
    else:
        logger.warning(f"Could not create 'New Mexico' column: missing one or more required source columns: {newMexico_regions}")
    
    # Rename United States to Total US
    if "United States" in result_df.columns:
        result_df = result_df.rename(columns={"United States": "Total US"})
        logger.info("Renamed 'United States' column to 'Total US'")
    else:
        logger.warning("Could not rename 'United States' to 'Total US': column not found")
    
    return result_df

def main():
    """Main function to fetch and process Genscape data"""
    try:
        # Load configuration
        logger.info("Loading configuration...")
        config = load_config()
        
        if not config:
            return pd.DataFrame()
        
        # Initialize API client
        client = OilForecastAPI(config["api_key"])
        
        # Get report dates based on start date
        report_dates = client.get_report_dates(config.get("start_date"))
        
        if not report_dates:
            logger.error("No report dates available. Exiting.")
            return pd.DataFrame()
            
        # Get forecast data for each report date
        all_data = []
        for report_date in report_dates:
            df = client.get_forecast_data(report_date)
            if not df.empty:
                all_data.append(df)
                logger.info(f"Successfully retrieved data for report date: {report_date}")
            else:
                logger.warning(f"No data available for report date: {report_date}")
            # Add delay between requests
            time.sleep(3)
        
        if not all_data:
            logger.error("No forecast data retrieved. Exiting.")
            return pd.DataFrame()
        
        # Combine all data
        combined_df = pd.concat(all_data, ignore_index=True)
        
        # Transform to wide format with MMM-YY report date format
        logger.info("Transforming data to wide format")
        wide_format = client.transform_to_wide_format(combined_df)
        
        # Add calculated regions and rename columns
        logger.info("Adding calculated region columns")
        wide_format = add_calculated_regions(wide_format)
        
        logger.info(f"Process completed successfully. Retrieved {len(wide_format)} forecast rows.")
        return wide_format
        
    except Exception as e:
        logger.error(f"An error occurred during execution: {str(e)}")
        # Print stacktrace for debugging
        import traceback
        logger.error(traceback.format_exc())
        return pd.DataFrame()

if __name__ == "__main__":
    df = main()
    print("\nPreview of the data:")
    print(df.head())