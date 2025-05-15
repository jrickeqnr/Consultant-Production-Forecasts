# Standard Library Imports
import logging
import os
import re
import json

# Third-Party Libraries
import pandas as pd

# PySpark Imports
from pyspark.dbutils import DBUtils
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.utils import AnalysisException

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SQLWriter:
    def __init__(self, environment: str = 'prod', config_path: str = 'config.json'):
        self.environment = environment
        
        # Load configuration
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
                sql_config = config.get('sql', {})
                self.target_table_name = sql_config.get('target_table', 'Consultant_Production_Forecasts')  # Default if not found
        except Exception as e:
            logger.warning(f"Error loading config from {config_path}, using default table name: {str(e)}")
            self.target_table_name = 'Consultant_Production_Forecasts'  # Default table name
        
        self.username = f"f_datahub_{environment}@statoil.net"
        self.password = dbutils.secrets.get(scope=f"{environment}_Datahub_Scope", key=f"f-datahub-{environment}")
        self.database = "datahub"
        self.sql_server_name = f"datasql{environment}ussc.database.windows.net"
        
        # Set up JDBC URL
        self.jdbc_url = (
            f"jdbc:sqlserver://{self.sql_server_name}:1433;database={self.database};"
            f"user={self.username};password={self.password};"
            f"trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;"
            f"authentication=ActiveDirectoryPassword;loginTimeout=30"
        )

    def truncate_table(self):
        """Truncate the target table before writing new data"""
        jdbc_properties = {
            "user": self.username,
            "password": self.password,
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        }

        Properties = spark._jvm.java.util.Properties
        jdbc_props = Properties()
        for key, value in jdbc_properties.items():
            jdbc_props.setProperty(key, value)

        # Create the JDBC connection
        try:
            connection = spark._jvm.java.sql.DriverManager.getConnection(self.jdbc_url, jdbc_props)
            statement = connection.createStatement()
            statement.execute(f"TRUNCATE TABLE {self.target_table_name}")
            logger.info(f"Table {self.target_table_name} truncated successfully.")
        except Exception as e:
            logger.error(f"Error truncating table {self.target_table_name}: {e}")
        finally:
            # Ensure resources are closed
            if statement is not None:
                statement.close()
            if connection is not None:
                connection.close()

    def write_to_sql(self, spark_df: DataFrame) -> None:
        """
        Write a Spark DataFrame to SQL Server
        
        Args:
            spark_df: Spark DataFrame to write
        """
        try:
            # Truncate the table first
            self.truncate_table()

            # Write the PySpark DataFrame to the SQL Server table
            spark_df.write \
                .format("jdbc") \
                .option("url", self.jdbc_url) \
                .option("dbtable", self.target_table_name) \
                .option("user", self.username) \
                .option("password", self.password) \
                .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
                .mode("append") \
                .save()
                
            logger.info(f"Data written to table {self.target_table_name} successfully.")
        except Exception as e:
            logger.error(f"Error writing to SQL Server: {e}")
            raise