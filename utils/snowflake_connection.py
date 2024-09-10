import snowflake.connector
import re
from secrets_manager import get_secrets
from config_variables import snowflake_config
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

def get_snowflake_pkb(option):
    """
    Generate Snowflake private key bytes based on the given option (CONNECTOR or SPARK).
    :param option: str, either "CONNECTOR" or "SPARK"
    :return: str, private key bytes
    """
    secret_names = ["snowflake/emea/privateKey", "snowflake/emea/passphrase"]  # See in AWS Secrets Manager
    region_name = "eu-west-1"

    secrets = get_secrets(secret_names, region_name)

    secret = secrets["snowflake/emea/privateKey"]
    passphrase = secrets["snowflake/emea/passphrase"]

    p_key = serialization.load_pem_private_key(
        secret.encode("utf-8"),
        password=passphrase.encode(),
        backend=default_backend(),
    )

    if option == "CONNECTOR": # Snowflake connector
        pkb = p_key.private_bytes(
            encoding=serialization.Encoding.DER, # Returns the private key in DER format. 
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption())

    elif option == "SPARK": # Spark
        pkb = p_key.private_bytes(
            encoding=serialization.Encoding.PEM, # Returns the private key in PEM format.
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption())

        # Convert the private key bytes to a string and remove "comments"
        pkb = pkb.decode("UTF-8")
        pkb = re.sub("-*(BEGIN|END) PRIVATE KEY-*\n", "", pkb).replace("\n", "")

    else:
        raise ValueError("Invalid option provided. Expected 'READ' or 'WRITE'.")

    return pkb


def create_snowflake_connection(pkb):
    """
    Create a Snowflake connection using the provided private key bytes.
    
    :param pkb: str, private key bytes
    :return: snowflake.connector.connection object
    """
    return snowflake.connector.connect(
        user=snowflake_config['user'],
        account=snowflake_config['account'],
        private_key=pkb,
        warehouse=snowflake_config['warehouse'],
        database=snowflake_config['database'],
        schema=snowflake_config['schema'],
        role=snowflake_config['role']
    )