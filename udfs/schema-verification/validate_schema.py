import json
import logging
import os
from datetime import datetime

import boto3
from .compare_schemas import compare_schemas
from .fetch_expected_schema_from_s3 import (fetch_expected_schema_from_s3,
                                           setup_logging)
from .query_snowflake_schema import query_snowflake_schema
from utils.snowflake_connection import get_snowflake_pkb


def save_result_to_json(result, output_path):
    """Saves the schema comparison result to a JSON file."""
    with open(output_path, 'w') as json_file:
        json.dump(result, json_file, indent=4)
    logging.info(f"Schema comparison result saved to {output_path}")


def save_result_to_s3(result, bucket_name, output_key):
    """Saves the schema comparison result to an S3 bucket."""
    s3 = boto3.client('s3')
    result_json = json.dumps(result, indent=4)
    s3.put_object(Body=result_json, Bucket=bucket_name, Key=output_key)
    logging.info(f"Schema comparison result saved to s3://{bucket_name}/{output_key}")

#  # Step 4: Store the result in S3 as JSON
# s3_client = boto3.client('s3')
# s3_key = f"data-verification-results/{table_name_source}_vs_{table_name_target}_{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}.json"
# s3_client.put_object(
#     Bucket=s3_output_bucket,
#     Key=s3_key,
#     Body=json.dumps(result, indent=4),
#     ContentType="application/json"
# )


def handle_schema_comparison(save_to_s3=False):
    """Main function to handle schema comparison and saving result."""

    logger = setup_logging()

    # Fetch Snowflake private key bytes (replace with your own method
    pkb = get_snowflake_pkb("CONNECTOR")

    # Define the S3 bucket and key where the expected schema is stored
    bucket_name = 'athena-dwh-queries'
    expected_schema_s3_key = 'snowflake-landing-schemas/CT_COUNTRY_schema.json'

    # Define the Snowflake table, schema, and database to validate
    table_name = 'CT_COUNTRY'
    schema_name = 'LANDING_OMACL_SCHEMA'
    database_name = 'DEV_OMACL_DB'
    
    # Step 1: Fetch expected schema from S3
    # expected_schema = fetch_expected_schema_from_s3(bucket_name, expected_schema_s3_key, logger)

    with open('C:/Users/sortrufa/OneDrive - Boehringer Ingelheim/Documents/table_schemas/ct_country/expected_schema_CT_COUNTRY.json', 'r') as f:
        expected_schema = json.load(f)

    # Step 2: Query the actual schema from Snowflake
    actual_schema = query_snowflake_schema(pkb, table_name, schema_name, database_name, logger)
    
    # Step 3: Perform schema comparison
    comparison_result = compare_schemas(table_name, expected_schema, actual_schema, logger)
    
    # Step 4: Prepare output path
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file_name = f"schema_comparison_{table_name}_{timestamp}.json"
    
    if save_to_s3:
        # Save result to S3
        output_s3_key = f"schema_comparison_results/{output_file_name}"
        save_result_to_s3(comparison_result, bucket_name, output_s3_key)
    else:
        # Save result locally
        # output_path = os.path.join(os.getcwd(), output_file_name)
        output_path = os.path.join('C:/Users/sortrufa/OneDrive - Boehringer Ingelheim/Documents/projects/data-verification/verification-results/', output_file_name)
        save_result_to_json(comparison_result, output_path)

    return comparison_result


# Example usage:
# Run schema comparison and save result locally (or to S3 if needed)
handle_schema_comparison(save_to_s3=False)

