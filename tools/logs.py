import sys
import logging

def logger_settings():
    # Configure logging
    LOG_FORMAT = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    logging.basicConfig(
        level=logging.INFO,  # Default level
        format=LOG_FORMAT,
        handlers=[
            logging.FileHandler("lead_to_sale_ETL.log"),  # Log to a file
            logging.StreamHandler(sys.stdout),  # Log to console
        ],
    )

    # Create a logger
    logger = logging.getLogger("ETL")
    return logger