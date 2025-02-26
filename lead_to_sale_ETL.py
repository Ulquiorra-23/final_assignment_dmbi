# Importing standard libraries
import os

# Importing third party libraries
import yaml
import pandas as pd

# Importing custom libraries
from tools.sql_tools import write_to_database
from tools.logs import log_wrap


@log_wrap
def extract_data(logger):
    try:
        logger.info('Starting data extraction...')
        # Extraction logic
        logger.info('Data extraction completed successfully.')
        return data
    except Exception as e:
        logger.error(f'Error during extraction: {e}', exc_info=True)
        raise  # Re-raise the exception

@log_wrap
def transform_data(data,logger):
    try:
        logger.info('Starting data transformation...')
        # Transformation logic
        logger.info('Data transformation completed.')
        return transformed_data
    except Exception as e:
        logger.error(f'Transformation error: {e}', exc_info=True)
        raise

@log_wrap
def load_data(transformed_data,logger):
    try:
        logger.info('Starting data load...')
        # Load logic (write to SQL)
        logger.info('Data load completed successfully.')
    except Exception as e:
        logger.error(f'Load error: {e}', exc_info=True)
        raise

@log_wrap
def main(logger):
    logger.info('ETL process started.')
    data = extract_data()
    transformed_data = transform_data(data)
    load_data(transformed_data)
    logger.info('ETL process completed successfully.')

if __name__ == '__main__':
    main()

