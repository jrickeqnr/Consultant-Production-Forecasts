import pyodbc
import pandas as pd
import sqlalchemy as sa
from typing import Optional
import logging
from datetime import datetime
import json
from pyspark.dbutils import DBUtils

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AzureSQLConnector:
    def __init__(
        self,
        environment: str = 'prod',
        config_path: str = 'config.json'
    ):
        """
        Initialize the SQL connector with environment specific configuration
        
        Args:
            environment: Environment to use ('prod' or 'dev')
            config_path: Path to config file
        """
        self.environment = environment
        self.connection = None
        self.sa_engine = None
        
        # Load SQL configuration
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
                sql_config = config.get('sql', {})
                self.database = sql_config.get('database', 'datahub')
        except Exception as e:
            logger.warning(f"Error loading config from {config_path}, using default database: {str(e)}")
            self.database = 'datahub'
        
        # Set up database connection parameters using secrets
        self.username = f"f_datahub_{environment}@statoil.net"
        self.password = dbutils.secrets.get(scope=f"{environment}_Datahub_Scope", key=f"f-datahub-{environment}")
        self.sql_server_name = f"datasql{environment}ussc.database.windows.net"
        
        # Set up JDBC URL
        self.connection_string = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={self.sql_server_name};"
            f"DATABASE={self.database};"
            f"UID={self.username};"
            f"PWD={self.password};"
            "TrustServerCertificate=false;"
            "hostNameInCertificate=*.database.windows.net;"
            "authentication=ActiveDirectoryPassword"
        )

    def connect(self) -> None:
        """
        Establishes connection to Azure SQL database.
        Will prompt for authentication in browser window.
        """
        try:
            self.connection = pyodbc.connect(self.connection_string)
            self.sa_engine = sa.create_engine('mssql+pyodbc://', creator=lambda: self.connection)
            logger.info("Successfully connected to Azure SQL database")
            
        except Exception as e:
            logger.error(f"Failed to connect to database: {str(e)}")
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
            logger.info(f"Query executed successfully. Returned {len(df)} rows")
            return df
            
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            return None

    def close(self) -> None:
        """Closes database connection"""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

def process_eia_data():
    """Main function to process EIA data and generate wide format output"""
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
            'COPRPUS': 'U.S.', # United States
            'COPRR48': 'Rest of Lower 48',
            'PAPR48NGOM': 'Lower 48 (excl GOM)',
            'PAPRPAK': 'Alaska',
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

        # Reformat dates to MMM-YY
        eia_df_wide['reportDate'] = pd.to_datetime(eia_df_wide['reportDate']).dt.strftime('%b-%y')
        eia_df_wide['month'] = pd.to_datetime(eia_df_wide['month']).dt.strftime('%b-%y')

        # Reorder columns
        desired_order = ['reportDate', 'month'] + sorted(location_mapping.values())
        eia_df_wide = eia_df_wide[desired_order]

        # Save to CSV
        output_file = f"oil_production_forecasts_eia_{datetime.now().strftime('%Y%m%d')}.csv"
        eia_df_wide.to_csv(output_file, index=False)
        print(f"Data saved to {output_file}")

        # Display results
        print("\nPreview of the processed data:")
        print(eia_df_wide)

    except Exception as e:
        logging.error(f"Error processing EIA data: {str(e)}")
        raise

    finally:
        connector.close()

if __name__ == "__main__":
    df = process_eia_data()
    print("\nPreview of the processed data:")
    print(df)