# Importing standard libraries
import os

# Importing third party libraries
import yaml
import pandas as pd

# Importing custom libraries
from tools.sql_tools import write_to_database
from tools.logs import log_wrap

# Paths

# Names

# Data Types

# Final Column Names
FINAL_NAMES_WEATHER = {'date':'year','temperature': 'avg_temperature', 'relative_humidity': 'avg_relative_humidity',
                                               'precipitation_rate':'avg_precipitation_rate','wind_speed':'avg_wind_speed'}

# Final Columns
FINAL_COLS_SALES = ['sales_id','zipcode_id','lead_id','financing_type','current_phase','phase_pre_ko',
              'is_modified','offer_sent_date','contract_1_dispatch_date','contract_2_dispatch_date','contract_1_signature_date',
              'contract_2_signature_date','most_recent_contract_signature','visit_date','technical_review_date',
              'project_validation_date','sale_dismissal_date','ko_date','visiting_company','ko_reason',
              'installation_peak_power_kw','installation_price','n_panels','cusomer_type']
FINAL_COLS_WEATHER=['weather_id','zipcode_id','year','avg_temperature','avg_relative_humidity','avg_precipitation_rate',
                    'avg_wind_speed']

# @Juanjo
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

# @Max
@log_wrap
def transform_data(data: list, logger) -> list:
    '''
    Takes a list of dfs as arguments of size 3 and returns a list of transformed dataframes
    Order:
    [0] = Sales
    [1] = Zipcode
    [2] = Weather
    '''
    try:
        logger.info('Reading Dataframes...')
        sales_fact_df_raw = data[0]
        logger.info(f'Sales Data has {len(sales_fact_df_raw)} records.')
        zipcode_dim_df_raw = data[1]
        logger.info(f'Zipcode Data has {len(zipcode_dim_df_raw)} records.')
        weather_dim_df_raw = data[2]
        logger.info(f'Weather Data has {len(weather_dim_df_raw)} records.')
        
        logger.info(f'Processing Transformations...')
        sales_fact_df_raw.columns = sales_fact_df_raw.columns.str.lower()
        zipcode_dim_df_raw.columns = zipcode_dim_df_raw.columns.str.lower()
        weather_dim_df_raw.columns = weather_dim_df_raw.columns.str.lower()
        
        logger.info(f'Creating a PK in zipcode_dim_df_raw...')
        zipcode_dim_df_raw.insert(0,'zipcode_id',range(1, len(zipcode_dim_df_raw) + 1))
        zipcode_dim_df_raw['zipcode_id'] = zipcode_dim_df_raw['zipcode_id'].astype('int32')
        zipcode_dim_df = zipcode_dim_df_raw
        
        logger.info(f'Grouping weather_dim_df_raw...')
        weather_dim_df_raw['date'] = weather_dim_df_raw['date'].dt.year
        weather_dim_df_raw = weather_dim_df_raw.groupby(['date','zipcode']).mean().reset_index()
        
        logger.info(f'Adding FK zipcode_id in weather table...')
        weather_dim_df = pd.merge(weather_dim_df,zipcode_dim_df_raw,on= 'zipcode', how='left')
        
        logger.info(f'Dropping null zipcode_id from weather table...')
        weather_dim_df = weather_dim_df_raw.dropna()
        
        logger.info(f'Creating a PK in weather_dim_df_raw...')
        weather_dim_df.insert(0,'weather_id',range(1, len(weather_dim_df) + 1))
        weather_dim_df['weather_id'] = weather_dim_df['weather_id'].astype('int32')
        
        logger.info(f'Creating a PK in sales_fact_df_raw...')
        sales_fact_df_raw.insert(0,'sales_id',range(1, len(sales_fact_df_raw) + 1))
        sales_fact_df_raw['sales_id'] = sales_fact_df_raw['sales_id'].astype('int32')
        
        logger.info(f'Adding calculated column most_recent_contract_signature to sales_fact_df...')
        sales_fact_df_raw.insert(16,'most_recent_contract_signature', \
            sales_fact_df_raw[['contract_1_signature_date', 'contract_2_signature_date']].max(axis=1))

        logger.info(f'Adding FK zipcode_id in sales table...')
        sales_fact_df = pd.merge(sales_fact_df_raw, zipcode_dim_df_raw, on='zipcode', how='left')
        
        logger.info(f'Handling column types, names and selection...')
        weather_dim_df = weather_dim_df.rename(columns=FINAL_NAMES_WEATHER)
        weather_dim_df = weather_dim_df[FINAL_COLS_WEATHER]
        sales_fact_df = sales_fact_df[FINAL_COLS_SALES]        
        
        logger.info(f'Packing data for loading...')
        list_of_transformed_dfs = [zipcode_dim_df,weather_dim_df, sales_fact_df]
        
        return list_of_transformed_dfs
    
    except Exception as e:
        logger.error(f'Transformation error: {e}', exc_info=True)
        raise

# @Jakob Code
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

