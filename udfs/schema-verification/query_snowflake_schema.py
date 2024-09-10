import snowflake.connector
from utils.secrets_manager import get_secrets
from utils.snowflake_connection import create_snowflake_connection, get_snowflake_pkb


def query_snowflake_schema(pkb, table_name, schema_name, database_name, logger):
    """
    Queries Snowflake for the schema of the given table using INFORMATION_SCHEMA.COLUMNS.
    
    Parameters:
        pkb (bytes): Private key bytes for Snowflake connection.
        table_name (str): Name of the table to query.
        schema_name (str): Name of the schema containing the table.
        database_name (str): Name of the database containing the schema.
        logger (logging.Logger): Logger for logging messages.
    
    Returns:
        list: List of dictionaries representing the actual schema.
    """
    sql_statement = f"""
    SELECT 
        COLUMN_NAME, 
        IS_NULLABLE, 
        DATA_TYPE, 
        CHARACTER_MAXIMUM_LENGTH, 
        NUMERIC_PRECISION, 
        NUMERIC_SCALE, 
        DATETIME_PRECISION
    FROM {database_name}.INFORMATION_SCHEMA.COLUMNS
    WHERE 
        table_name = '{table_name.upper()}'
        AND table_schema = '{schema_name.upper()}'
        AND table_catalog = '{database_name.upper()}';
    """
    
    logger.info(f"Executing Snowflake schema query for table '{database_name}.{schema_name}.{table_name}'.")
    
    try:
        with create_snowflake_connection(pkb) as ctx:
            """
            The connection context (ctx) is managed by a with statement, 
            ensuring that the connection is properly closed once the query is done, 
            even if an error occurs.
            """
            with ctx.cursor() as cs:
                cs.execute(sql_statement) # execute the SQL query
                snowflake_schema = cs.fetchall() # fetch all the rows from the result
                """
                cs.fetchall(): After the query is executed, fetchall retrieves all the rows returned by the query. 
                Each row contains the schema information for one column in the table. 
                The result is stored in the snowflake_schema variable, which is a list of tuples.
                """
        
        # Convert the fetched data into a structured format similar to the expected JSON schema
        actual_schema = []
        for row in snowflake_schema:
            actual_schema.append({
                "COLUMN_NAME": row[0],# First field of the tuple. row[n] refers to accessing the elements of each row (tuple) returned from the SQL query. 
                "IS_NULLABLE": row[1],
                "DATA_TYPE": row[2],
                "CHARACTER_MAXIMUM_LENGTH": row[3],
                "NUMERIC_PRECISION": row[4],
                "NUMERIC_SCALE": row[5],
                "DATETIME_PRECISION": row[6]
            })
        
        logger.info("Successfully retrieved the actual schema from Snowflake.")
        return actual_schema
    
    except snowflake.connector.Error as e:
        logger.error(f"Snowflake query failed: {e}")
        raise

# # Example usage of the query_snowflake_schema function
# from fetch_expected_schema_from_s3 import setup_logging
# def example_usage():
#     # Retrieve the private key bytes for Snowflake connection
#     pkb = get_snowflake_pkb("CONNECTOR")  # Assumes the private key is fetched via your secrets manager

#     logger = setup_logging()

#     # Define the table, schema, and database names
#     table_name = 'CT_COUNTRY'
#     schema_name = 'LANDING_OMACL_SCHEMA'
#     database_name = 'DEV_OMACL_DB'

#     # Query the Snowflake schema
#     actual_schema = query_snowflake_schema(pkb, table_name, schema_name, database_name, logger)

#     # Print the schema information retrieved from Snowflake
#     print("Schema for table:", table_name)
#     # for col in actual_schema:
#     #     print(col)
    
#     print("\n")
#     print(actual_schema)

# # Run the example to see the output
# example_usage()


