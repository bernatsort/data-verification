import boto3
from botocore.exceptions import ClientError

def get_secrets(secret_names: list, region: str) -> str:
    """
    Retrieve secrets from AWS Secrets Manager.
    
    This function retrieves the specified secrets from AWS Secrets Manager and returns them in a dictionary.
    The secrets are stored with their respective secret names as keys.
    
    Args:
        None

    Returns:
        dict: A dictionary containing the retrieved secrets with their secret names as keys.
        
    Raises:
        ClientError: If there is an error while retrieving the secrets from AWS Secrets Manager.
    """
    
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region
    )

    secrets = {}
    for secret_name in secret_names:
        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name
            )
        except ClientError as e:
            # For a list of exceptions thrown, see
            # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
            raise e

        # Decrypts secret using the associated KMS key.
        secrets[secret_name] = get_secret_value_response['SecretString']

    return secrets


