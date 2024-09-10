def compare_schemas(table_name, expected_schema, actual_schema, logger):
    """
    Compares the expected and actual schemas and identifies mismatches.
    
    Parameters:
        expected_schema (list): List of dictionaries representing the expected schema.
        actual_schema (list): List of dictionaries representing the actual schema.
        logger (logging.Logger): Logger for logging messages.
    
    Returns:
        dict: Dictionary containing lists of mismatches, missing columns, and extra columns.
    """
    logger.info("Starting schema comparison.")
    
    # Convert lists to dictionaries keyed by COLUMN_NAME for easier comparison
    expected_dict = {col['COLUMN_NAME'].upper(): col for col in expected_schema}
    actual_dict = {col['COLUMN_NAME'].upper(): col for col in actual_schema}
    
    mismatches = [] # mismatches: To store any mismatched attributes between the expected and actual schema.
    missing_columns = [] # missing_columns: To track columns that exist in the expected schema but are missing in the actual schema.
    extra_columns = [] # extra_columns: To track columns that exist in the actual schema but were not expected.
    
    # Check for missing columns and mismatches
    for column_name, expected_col in expected_dict.items(): # iterate over each column in the expected schema
        actual_col = actual_dict.get(column_name) # for each column in the expected schema, looks for the same column in the actual schema. 
                                                  # If the column is not found, this means the column is missing from the actual schema. 
        if not actual_col:
            missing_columns.append(column_name) # if the column is not found, it is added to the missing columns list. 
            logger.warning(f"Missing column in actual schema: {column_name}")
            continue # the function continues to the next column without performing further comparisons on this missing column. 
        
        # Compare each attribute
        # Once a column exists in both the actual and expected schema, we begin comparing each attribute. 
        for key in ['DATA_TYPE', 'IS_NULLABLE', 'CHARACTER_MAXIMUM_LENGTH', 
                    'NUMERIC_PRECISION', 'NUMERIC_SCALE', 'DATETIME_PRECISION']: # attributes to be compared for each column. 
            expected_value = expected_col.get(key) # retrieves the expected value of the current attribute from the expected schema.
            actual_value = actual_col.get(key) # retrieves the actual value for the same attribute from the actual schema. 
            
            # # Normalize None and zero values if necessary
            # if expected_value is None:
            #     expected_value_normalized = None
            # elif isinstance(expected_value, float) and expected_value.is_integer():
            #     expected_value_normalized = int(expected_value)
            # else:
            #     expected_value_normalized = expected_value
            
            # if actual_value is None:
            #     actual_value_normalized = None
            # elif isinstance(actual_value, float) and actual_value.is_integer():
            #     actual_value_normalized = int(actual_value)
            # else:
            #     actual_value_normalized = actual_value

            # Identifying mismatches
            # If the expected value and the actual value are different, it is considered a mismatch. 
            if expected_value != actual_value:
                mismatch_detail = {
                    'COLUMN_NAME': column_name,
                    'ATTRIBUTE': key,
                    'EXPECTED': expected_value,
                    'ACTUAL': actual_value
                }
                mismatches.append(mismatch_detail)
                logger.error(f"Mismatch in column '{column_name}' for attribute '{key}': Expected '{expected_value}', Got '{actual_value}'")
    
    # Check for extra columns in the actual schema
    # If a column exists in the actual schema but not in the expected schema, it is considered an extra column. 
    # for column_name in actual_dict:
    #     if column_name not in expected_dict:
    #         extra_columns.append(column_name)
    #         logger.warning(f"Extra column found in actual schema: {column_name}")
    
    logger.info("Schema comparison completed.")
    
    comparison_result = {
        'schema_check_status': "FAIL" if mismatches or missing_columns else "PASS", # or extra_columns 
        'table': table_name,
        'mismatches': mismatches,
        'missing_columns': missing_columns
        # ,'extra_columns': extra_columns
    }

    
    # if comparison_result['mismatches'] or comparison_result['missing_columns'] or comparison_result['extra_columns']:
    #     logger.error(f"Schema check failed for table {table_name}. See details below:")
    #     logger.error(json.dumps(comparison_result, indent=4))
    #     # Send alert via email or Slack
    # else:
    #     logger.info(f"Schema check passed for table {table_name}.")
    
    return comparison_result

# # Example usage 
# import json
# from query_snowflake_schema import query_snowflake_schema
# from snowflake_count_rows import get_snowflake_pkb
# from fetch_expected_schema_from_s3 import setup_logging
# logger = setup_logging()
# # Load the expected schema from a JSON file
# # # without errors
# # with open('C:/Users/sortrufa/OneDrive - Boehringer Ingelheim/Documents/table_schemas/ct_country/expected_schema_CT_COUNTRY.json', 'r') as f:
# #     expected_schema = json.load(f)
# # with errors
# with open('C:/Users/sortrufa/OneDrive - Boehringer Ingelheim/Documents/table_schemas/ct_country/error_expected_schema_CT_COUNTRY.json', 'r') as f:
#     expected_schema = json.load(f)

# pkb = get_snowflake_pkb("CONNECTOR")  # Assumes the private key is fetched via your secrets manager

# # Define the table, schema, and database names
# table_name = 'CT_COUNTRY'
# schema_name = 'LANDING_OMACL_SCHEMA'
# database_name = 'DEV_OMACL_DB'

# # Get the actual schema from Snowflake
# actual_schema = query_snowflake_schema(pkb, table_name, schema_name, database_name, logger)

# # Compare the schemas
# mismatches = compare_schemas(expected_schema, actual_schema, logger)
# # print(mismatches)

# Output schema with errors: 
"""
2024-09-09 18:54:16,514 - SchemaComparator - INFO - Executing Snowflake schema query for table 'DEV_OMACL_DB.LANDING_OMACL_SCHEMA.CT_COUNTRY'.
2024-09-09 18:54:18,785 - SchemaComparator - INFO - Successfully retrieved the actual schema from Snowflake.
2024-09-09 18:54:18,786 - SchemaComparator - INFO - Starting schema comparison.
2024-09-09 18:54:18,786 - SchemaComparator - ERROR - Mismatch in column 'INGESTION_TIMESTAMP' for attribute 'NUMERIC_PRECISION': Expected '13.0', Got '14'
2024-09-09 18:54:18,786 - SchemaComparator - WARNING - Missing column in actual schema: INT_CRE_T
2024-09-09 18:54:18,787 - SchemaComparator - ERROR - Mismatch in column 'AUDIT_TASK_ID' for attribute 'IS_NULLABLE': Expected 'NO', Got 'YES'
2024-09-09 18:54:18,787 - SchemaComparator - ERROR - Mismatch in column '_HOODIE_COMMIT_TIME' for attribute 'DATA_TYPE': Expected 'DATE', Got 'TEXT'
2024-09-09 18:54:18,787 - SchemaComparator - WARNING - Missing column in actual schema: NEW_COLUMN
2024-09-09 18:54:18,787 - SchemaComparator - INFO - Schema comparison completed.
2024-09-09 18:54:18,788 - SchemaComparator - ERROR - Schema check failed for table CT_COUNTRY. See details below:
2024-09-09 18:54:18,788 - SchemaComparator - ERROR - {
    "mismatches": [
        {
            "COLUMN_NAME": "INGESTION_TIMESTAMP",
            "ATTRIBUTE": "NUMERIC_PRECISION",
            "EXPECTED": 13.0,
            "ACTUAL": 14
        },
        {
            "COLUMN_NAME": "AUDIT_TASK_ID",
            "ATTRIBUTE": "IS_NULLABLE",
            "EXPECTED": "NO",
            "ACTUAL": "YES"
        },
        {
            "COLUMN_NAME": "_HOODIE_COMMIT_TIME",
            "ATTRIBUTE": "DATA_TYPE",
            "EXPECTED": "DATE",
            "ACTUAL": "TEXT"
        }
    ],
    "missing_columns": [
        "INT_CRE_T",
        "NEW_COLUMN"
    ],
    "extra_columns": [],
    "schema_check_status": "FAIL"
}

"""
# Output schema without errors: 
"""
2024-09-09 18:55:13,055 - SchemaComparator - INFO - Executing Snowflake schema query for table 'DEV_OMACL_DB.LANDING_OMACL_SCHEMA.CT_COUNTRY'.
2024-09-09 18:55:16,001 - SchemaComparator - INFO - Successfully retrieved the actual schema from Snowflake.
2024-09-09 18:55:16,001 - SchemaComparator - INFO - Starting schema comparison.
2024-09-09 18:55:16,002 - SchemaComparator - INFO - Schema comparison completed.
2024-09-09 18:55:16,002 - SchemaComparator - INFO - Schema check passed for table CT_COUNTRY.
"""



