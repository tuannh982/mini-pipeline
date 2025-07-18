import boto3


def s3_client(config):
    endpoint = config.get("endpoint")
    access_key = config.get("accessKey")
    secret_key = config.get("secretKey")

    if endpoint is None:
        raise AttributeError("'endpoint' is not defined")
    if access_key is None:
        raise AttributeError("'access_key' is not defined")
    if secret_key is None:
        raise AttributeError("'secret_key' is not defined")

    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    return s3


def create_bucket_if_not_exists(s3, bucket: str) -> None:
    try:
        s3.head_bucket(Bucket=bucket)
    except:
        s3.create_bucket(Bucket=bucket)
