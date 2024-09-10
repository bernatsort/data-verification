import json
import logging

import boto3
from botocore.exceptions import ClientError


def setup_logging(log_level=logging.INFO):
    """Sets up the logging configuration."""
    logger = logging.getLogger('SchemaComparator')
    logger.setLevel(log_level)
    
    # Create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(log_level)
    
    # Create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    
    # Add the handlers to the logger
    if not logger.handlers:
        logger.addHandler(ch)
    
    return logger


def fetch_expected_schema_from_s3(bucket_name, key, logger):
    """
    Fetches the expected schema JSON from S3 and returns it as a list of dictionaries.
    
    Parameters:
        bucket_name (str): Name of the S3 bucket.
        key (str): S3 key (path) to the JSON file.
        logger (logging.Logger): Logger for logging messages.
    
    Returns:
        list: List of dictionaries representing the expected schema.
    """
    s3_client = boto3.client('s3')
    
    try:
        logger.info(f"Fetching expected schema from S3 bucket '{bucket_name}' with key '{key}'.")
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        content = response['Body'].read().decode('utf-8')
        expected_schema = json.loads(content)
        logger.info("Successfully fetched and parsed the expected schema.")
        return expected_schema
    except ClientError as e:
        logger.error(f"Failed to fetch expected schema from S3: {e}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse expected schema JSON: {e}")
        raise

# # Example usage: 
# bucket_name = 'athena-dwh-queries'
# key = 'snowflake-landing-schemas/CT_COUNTRY_schema.json'
# logger = setup_logging()
# expected_schema = fetch_expected_schema_from_s3(bucket_name, key, logger)
# print(expected_schema)

