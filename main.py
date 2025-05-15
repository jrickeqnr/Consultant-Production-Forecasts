import pandas as pd
from datetime import datetime
import logging
from typing import Dict
from pyspark.sql import SparkSession

# Import the processing functions from each module
from ea import main as ea_main
from genscape import main as genscape_main
from steo import process_eia_data
from write import SQLWriter

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def melt_and_add_source(df: pd.DataFrame, source: str) -> pd.DataFrame:
    """
    Melts the DataFrame to convert location columns to rows and adds a source column
    
    Args:
        df: DataFrame in wide format with reportDate, month, and location columns
        source: Name of the source (e.g., 'Energy Aspects', 'Genscape', 'EIA STEO')
        
    Returns:
        DataFrame in long format with columns: reportDate, month, location, value, source
    """
    # Get location columns (all columns except reportDate and month)
    location_cols = [col for col in df.columns if col not in ['reportDate', 'month']]
    
    # Melt the DataFrame
    melted_df = df.melt(
        id_vars=['reportDate', 'month'],
        value_vars=location_cols,
        var_name='location',
        value_name='value'
    )
    
    # Add source column
    melted_df['source'] = source
    
    return melted_df

def main():
    """Main function to process and combine all forecast sources"""
    try:
        logger.info("Starting forecast data collection and processing...")
        
        # Initialize Spark session
        spark = SparkSession.builder \
            .appName("Production Forecasts") \
            .getOrCreate()
        
        # Process Energy Aspects data
        logger.info("Processing Energy Aspects data...")
        ea_df = ea_main()
        if ea_df.empty:
            logger.warning("No Energy Aspects data available")
        
        # Process Genscape data
        logger.info("Processing Genscape data...")
        genscape_df = genscape_main()
        if genscape_df.empty:
            logger.warning("No Genscape data available")
        
        # Process EIA STEO data
        logger.info("Processing EIA STEO data...")
        steo_df = process_eia_data()
        if steo_df.empty:
            logger.warning("No EIA STEO data available")
        
        # Convert each DataFrame to long format and add source
        dataframes = []
        
        if not ea_df.empty:
            ea_long = melt_and_add_source(ea_df, "Energy Aspects")
            dataframes.append(ea_long)
            
        if not genscape_df.empty:
            genscape_long = melt_and_add_source(genscape_df, "Genscape")
            dataframes.append(genscape_long)
            
        if not steo_df.empty:
            steo_long = melt_and_add_source(steo_df, "EIA STEO")
            dataframes.append(steo_long)
        
        if not dataframes:
            logger.error("No data available from any source")
            return
        
        # Combine all DataFrames
        combined_df = pd.concat(dataframes, ignore_index=True)
        
        # Sort the DataFrame
        combined_df = combined_df.sort_values(['source', 'reportDate', 'month', 'location'])
        
        # Initialize SQL writer
        sql_writer = SQLWriter()
        
        # Convert pandas DataFrame to Spark DataFrame
        spark_df = spark.createDataFrame(combined_df)
        
        # Write data to SQL database
        logger.info("Writing data to SQL database...")
        sql_writer.write_to_sql(spark_df)
        
        # Display summary statistics
        logger.info("\nSummary of combined data:")
        logger.info(f"Total rows: {len(combined_df)}")
        logger.info("\nRows per source:")
        print(combined_df.groupby('source').size())
        
        # Clean up Spark session
        spark.stop()
        
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()