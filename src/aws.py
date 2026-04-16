import boto3
import json
from botocore.exceptions import ClientError
import io
import pandas as pd

def get_secret(secret_name, region_name="us-east-1"):
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise Exception

    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response['SecretString']

    # Parse the JSON string into a dictionary
    return json.loads(secret)

def get_s3_file(file, bucket='realitystats'):
    session = boto3.session.Session()
    s3 = session.client("s3")
    obj = s3.get_object(Bucket='realitystats', Key=file)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()))
    return df
