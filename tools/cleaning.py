# Importing standard libraries
import os
import logging

# Importing third party libraries
import pandas as pd


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Create logger instance
logger = logging.getLogger(__name__)



# SET PATHS OF 3 COOL CSVs
FILENAME_sales_phases_funnel_df = os.path.join(os.getcwd(), r'data/sale_phases_funnel.csv')
FILENAME_zipcode_df = os.path.join(os.getcwd(), r'data/zipcode_eae.csv')
FILENAME_meteo_df = os.path.join(os.getcwd(), r'data/meteo_eae.csv')


#SETTING TYPES
#Sales
SALES_TYPES = {'LEAD_ID':'str','FINANCING_TYPE':'str',
                    'CURRENT_PHASE':'str','PHASE_PRE_KO':'str',
                    'IS_MODIFIED':'bool','ZIPCODE':'str', 
                    'VISITING_COMPANY': 'str', 'KO_REASON': 'str', 
                    'INSTALLATION_PEAK_POWER_KW': 'float64', 
                    'INSTALLATION_PRICE': 'float', 
                    'N_PANELS': 'int', 'CUSOMER_TYPE': 'str' }

#Zipcosdes
ZIPCODE_TYPES = {'ZIPCODE':'str','ZC_LATITUDE':'float64',
                    'ZC_LONGITUDE':'float64','AUTONOMOUS_COMMUNITY':'str',
                    'AUTONOMOUS_COMMUNITY_NK':'str','PROVINCE':'str'}

#Meteo
METEO_TYPES = {'temperature': 'float', 'relative_humidity': 'float', 
            'precipitation_rate': 'float', 'wind_speed': 'float', 
            'zipcode': 'str' 
}



#CREATE 3 COOL DATAFRAMES FUNCTION
#Creates 3 super cool dataframes from the CSVs with the data types set from the start.


def dataFrameCreate():

    #SALES FUNNEL DATAFRAME

    #Dictionary with data types
   

    #Reading CSV to create dataframe with datatypes implemented from dictionary and 
    #additional date time datatypes.
    sales_phases_funnel_df = pd.read_csv(
        FILENAME_sales_phases_funnel_df, 
        delimiter=';', 
        dtype=SALES_TYPES,
        parse_dates=['OFFER_SENT_DATE', 'CONTRACT_1_DISPATCH_DATE', 
                    'CONTRACT_2_DISPATCH_DATE', 
                    'CONTRACT_1_SIGNATURE_DATE', 
                    'CONTRACT_2_SIGNATURE_DATE',
                    'VISIT_DATE',
                    'TECHNICAL_REVIEW_DATE',
                    'PROJECT_VALIDATION_DATE',
                    'SALE_DISMISSAL_DATE',
                    'KO_DATE'],
                    
        dayfirst=True  # This replaces the dayfirst=True in your to_datetime call
    )

    logger.info('sales_phases_funnel_df created')



    #ZIPCODE DATAFRAME

    # Reading CSV to create dataframe with datatypes implemented from dictionary
    zipcode_df = pd.read_csv(FILENAME_zipcode_df, delimiter=',', dtype=ZIPCODE_TYPES)


    logger.info('zipcodedf created')




    #METEO DATAFRAME

    # Reading CSV to create dataframe with datatypes implemented from dictionary and
    # additional date time datatype formatted to match the ones from the sales dataframe.
    meteo_df = pd.read_csv(FILENAME_meteo_df, delimiter=';',
        dtype=METEO_TYPES, parse_dates=['date'],  # Replace with actual column name
        date_format='%Y/%m/%d %H:%M:%S.%f'  # This matches your input format
    )

    logger.info('meteo_df created')
    list_of_dfs = [sales_phases_funnel_df, zipcode_df, meteo_df]

    return list_of_dfs
    



#list_of_dfs = dataFrameCreate()


#--------------------------------------------------------------


#DROPPING DUPLICATES(as they are not cool) FOR ALL DATAFRAMES

#creating the drop duplicate function
def dropDupli(dfs):
    #log
    logger.info(f'There are {dfs[0].duplicated().sum()} duplicate rows in sales_funnel_df before duplicate cleaning') 
    logger.info(f'There are {dfs[1].duplicated().sum()} duplicate rows in zipcode_df before duplicate cleaning')
    logger.info(f'There are {dfs[2].duplicated().sum()} duplicate rows in meteo_df before duplicate cleaning')  
    #DroppingDupli
    dfs[0].drop_duplicates(inplace=True)
    dfs[1].drop_duplicates(inplace=True)
    dfs[2].drop_duplicates(inplace=True)
    # Log after
    logger.info(f'There are {dfs[0].duplicated().sum()} duplicate rows in sales_funnel_df after duplicate cleaning') 
    logger.info(f'There are {dfs[1].duplicated().sum()} duplicate rows in zipcode_df after duplicate cleaning')
    logger.info(f'There are {dfs[2].duplicated().sum()} duplicate rows in meteo_df after duplicate cleaning')

    return dfs  # Return dfs instead of undefined variables



#list_of_dfs = dropDupli(list_of_dfs)



#-----------------------------------------------------------------




# --SALES FUNNEL DATAFRAME CLEANING FUCTIONS--


#DELETE UNUSABLE LEADS FUNCTION (This one is cool but we may not use) 

# Drop rows where KO_REASON is "Unreachable"
def delete_unreachable_leads(dfs):
    dfs[0] = dfs[0][~((dfs[0]['CURRENT_PHASE'] == 'KO') & (dfs[0]['KO_REASON'] == 'Unreachable'))]
    # Reset the index of the updated DataFrame
    dfs[0].reset_index(drop=True, inplace=True)
    logger.info('Unreachable leads deleted')
    return dfs

#list_of_dfs = delete_unreachable_leads(list_of_dfs)





#--------------------------------------------------------


# REMOVE "sales_phases_funnel_df[INSTALLATION PRICE]" OUTLIERS FUNCTION 

def delete_outliers(dfs):
    # Calculate Q1 (25th percentile) and Q3 (75th percentile)
    Q1 = dfs[0]['INSTALLATION_PRICE'].quantile(0.25)
    Q3 = dfs[0]['INSTALLATION_PRICE'].quantile(0.75)
    

    # Calculate the Interquartile Range (IQR)
    IQR = Q3 - Q1

    # Identify outliers inside a new Data Frame
    outliers_df = dfs[0][(dfs[0]['INSTALLATION_PRICE'] < (Q1 - 1.5 * IQR)) | 
                                     (dfs[0]['INSTALLATION_PRICE'] > (Q3 + 1.5 * IQR))]

    # Print the number of outliers
    logger.info('outliers_df dataframe created')
    logger.info(f'Number of outliers: {len(outliers_df)}')
    
    # Update sales_phases_funnel_df to exclude the outliers
    sales_phases_funnel_df = dfs[0][~((dfs[0]['INSTALLATION_PRICE'] < (Q1 - 1.5 * IQR)) | 
                                                  (dfs[0]['INSTALLATION_PRICE'] > (Q3 + 1.5 * IQR)))]
    logger.info('outliers removed from sales_phases_funnel_df')
    dfs.append(outliers_df)
    logger.info(f'outliers_df dataframe added to list_of_dfs')
    return dfs
    


#list_of_dfs = delete_outliers(list_of_dfs)
