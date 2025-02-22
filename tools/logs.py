import sys
import logging

def logger_writer(caller_name: str = 'MAIN'):
    '''
    Configures and returns the logger with a dynamic name based on the caller.
    '''
    LOG_FORMAT = '%(asctime)s - %(levelname)s - %(name)s - %(message)s'
    
    # Create a logger with a unique name per function
    logger = logging.getLogger(caller_name)
    if not logger.hasHandlers():
        logging.basicConfig(
            level=logging.INFO,  # Default logging level
            format=LOG_FORMAT,
            handlers=[
                logging.FileHandler('lead_to_sale_ETL.log'),  # Log to a file
                logging.StreamHandler(sys.stdout),  # Log to console
            ]
        )
    return logger

def log_wrap(func):
    '''
    A decorator that applies logging to each function call
    using the logger_writer function with dynamic function name.
    '''
    def wrapper(*args, **kwargs):
        # Get the caller function's name
        caller_name = func.__name__
        logger = logger_writer(caller_name)  # Get the logger for this function name
        logger.info(f'Executing function: {caller_name}')
        result = func(*args, **kwargs)
        logger.info(f'Completed function: {caller_name}')
        return result
    return wrapper