#!/usr/bin/env python3

import pyodbc
import pandas as pd
import sqlalchemy as sa
from typing import Optional
import logging
from datetime import datetime

class AzureSQLConnector:
    def __init__(
        self,
        server: str = 's426-geanasqlprod.database.windows.net',
        database: str = 'geanaprod',
        user_id: str = 'jrick@equinor.com',
        authentication: str = 'ActiveDirectoryInteractive'
    ):
        self.server = server
        self.database = database
        self.user_id = user_id
        self.authentication = authentication
        self.connection = None
        self.sa_engine = None
        
        # Set up logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def connect(self) -> None:
        """
        Establishes connection to Azure SQL database.
        Will prompt for authentication in browser window.
        """
        try:
            connection_string = (
                'DRIVER={ODBC Driver 18 for SQL Server};'
                f'SERVER={self.server};'
                f'DATABASE={self.database};'
                f'Authentication={self.authentication};'
                f'UID={self.user_id};'
                'TrustServerCertificate=no'
            )
            
            self.connection = pyodbc.connect(connection_string)
            self.sa_engine = sa.create_engine('mssql+pyodbc://', creator=lambda: self.connection)
            self.logger.info("Successfully connected to Azure SQL database")
            
        except Exception as e:
            self.logger.error(f"Failed to connect to database: {str(e)}")
            raise

    def query_to_df(self, query: str) -> Optional[pd.DataFrame]:
        """
        Executes SQL query and returns results as pandas DataFrame.
        
        Args:
            query (str): SQL query to execute
            
        Returns:
            Optional[pd.DataFrame]: Query results as DataFrame, None if query fails
        """
        try:
            if not self.connection or not self.sa_engine:
                self.connect()
                
            df = pd.read_sql(query, self.sa_engine)
            self.logger.info(f"Query executed successfully. Returned {len(df)} rows")
            return df
            
        except Exception as e:
            self.logger.error(f"Query execution failed: {str(e)}")
            return None

    def close(self) -> None:
        """Closes database connection"""
        if self.connection:
            self.connection.close()
            self.logger.info("Database connection closed")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

def process_eia_data():
    """Main function to process EIA data and return DataFrame in wide format"""
    # Initialize connector
    connector = AzureSQLConnector()

    # Define query
    query = """
    SELECT 
        series_id,
        Date AS month,
        data AS value,
        Edition AS reportDate
    FROM geana.eia_steo
    WHERE series_id IN (
        'COPRAP',
        'COPRBK',
        'COPREF',
        'COPRHA',
        'COPRPM',
        'COPRPUS',
        'COPRR48',
        'PAPR48NGOM',
        'PAPRPAK',
        'PAPRPGLF'
        )
        AND is_forecast = 'Forecast'
    ORDER BY month, series_id
    """

    # Fetch and process data
    try:
        # Fetch data into a DataFrame
        eia_df = connector.query_to_df(query)
        if eia_df is None:
            raise ValueError("Failed to retrieve data from database")

        # Standardize data
        eia_df['value'] = eia_df['value'] * 1000
        eia_df['units'] = 'KBD'

        # Create flow description mapping
        location_mapping = {
            'COPRAP': 'Appalachia',
            'COPRBK': 'Bakken',
            'COPREF': 'Eagle Ford',
            'COPRHA': 'Haynesville',
            'COPRPM': 'Permian',
            'COPRPUS': 'Total US',
            'COPRR48': 'Rest of Lower 48',
            'PAPR48NGOM': 'Lower 48 (excl GOM)',
            'PAPRPAK': 'PAPRPAK',
            'PAPRPGLF': 'Gulf of Mexico'
        }

        # Group and apply mappings
        eia_grouped = eia_df.groupby(['reportDate', 'month', 'series_id'])['value'].sum().reset_index()
        eia_grouped['location'] = eia_grouped['series_id'].map(location_mapping)

        # Create the wide format
        eia_df_wide = eia_grouped.pivot(
            index=['reportDate', 'month'],
            columns='location',
            values='value'
        ).reset_index()

        # Reformat dates to YYYY-MM
        eia_df_wide['reportDate'] = pd.to_datetime(eia_df_wide['reportDate']).dt.strftime('%Y-%m')
        eia_df_wide['month'] = pd.to_datetime(eia_df_wide['month']).dt.strftime('%Y-%m')

        # Reorder columns
        desired_order = ['reportDate', 'month'] + sorted(location_mapping.values())
        eia_df_wide = eia_df_wide[desired_order]

        return eia_df_wide

    except Exception as e:
        logging.error(f"Error processing EIA data: {str(e)}")
        return pd.DataFrame()

    finally:
        connector.close()

if __name__ == "__main__":
    df = process_eia_data()
    print("\nPreview of the processed data:")
    print(df)