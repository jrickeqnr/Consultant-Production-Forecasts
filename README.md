# Production Forecasts

This project combines oil production forecasts from multiple sources (EIA STEO, Energy Aspects, and Genscape) into a standardized format for analysis and comparison.

## Core Functionality

The program consists of several main components:

### 1. STEO Data Processing (`steo.py`)

- Connects to Azure SQL database using Databricks secrets
- Retrieves and processes EIA STEO forecast data
- Outputs standardized production forecasts by region

### 2. Energy Aspects API (`ea.py`)

- Interfaces with Energy Aspects API
- Implements rate limiting and retry mechanisms
- Retrieves production forecasts with location-based mapping
- Uses Databricks secrets for API authentication

### 3. Genscape API (`genscape.py`)

- Interfaces with Genscape's Oil Production Forecasting API
- Implements rate limiting
- Retrieves monthly US oil production forecasts
- Uses Databricks secrets for API authentication

### 4. Main Program (`main.py`)

- Orchestrates data collection from all sources
- Combines and standardizes forecast data
- Outputs combined forecasts to CSV format

## Deployment on Databricks

### Prerequisites

1. Access to a Databricks workspace
2. API keys for Energy Aspects and Genscape
3. Access to Azure SQL database

### Secret Configuration

Configure the following secrets in your Databricks workspace:

1. **API Secrets Scope** (for each environment: `prod_API_Scope` or `dev_API_Scope`):
   - `energy-aspects-api-key`: Energy Aspects API key
   - `genscape-api-key`: Genscape API key

2. **Datahub Scope** (for each environment: `prod_Datahub_Scope` or `dev_Datahub_Scope`):
   - `f-datahub-prod` or `f-datahub-dev`: Database credentials

### Deployment Steps

1. Create a new Databricks notebook or job
2. Upload all Python files to the Databricks workspace
3. Ensure all required Python packages are installed:

   ```python
   %pip install requests pandas urllib3
   ```

4. The code will automatically use Databricks secrets when running in the Databricks environment
5. For local development, the code falls back to using `config.json` for API keys

### Important Notes

- The code includes fallback mechanisms for local development using `config.json`
- Rate limiting is implemented for both APIs to prevent request throttling
- Logging is configured to track API interactions and data processing
- All timestamps are standardized to ensure consistent data comparison
- Output files are generated with datestamps for version control

## Error Handling

- All API interactions include proper error handling and logging
- Database connections are managed with proper connection pooling
- Rate limiting helps prevent API throttling
- Fallback mechanisms ensure code can run in both Databricks and local environments

## Security Considerations

- API keys and database credentials are stored securely in Databricks secrets
- SSL verification is properly handled for API requests
- Database connections use secure authentication methods
- Local `config.json` should never be committed to version control (add to .gitignore)
