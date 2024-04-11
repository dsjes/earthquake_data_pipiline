from contextlib import contextmanager
import boto3
import os
from dotenv import load_dotenv
load_dotenv()

@contextmanager
def s3_session():
    S3_ACCESS_KEY_ID = os.getenv("S3_ACCESS_KEY_ID")
    S3_SECRET_ACCESS_KEY = os.getenv("S3_SECRET_ACCESS_KEY")
    session = boto3.Session(
    aws_access_key_id=S3_ACCESS_KEY_ID,
    aws_secret_access_key=S3_SECRET_ACCESS_KEY
    )

    # Creating S3 Resource From the Session.
    s3 = session.resource('s3')
    try:
        yield s3
    finally:
        # No need to close the client for boto3
        pass

@contextmanager
def s3_client():
    S3_ACCESS_KEY_ID = os.getenv('S3_ACCESS_KEY_ID')
    S3_SECRET_ACCESS_KEY = os.getenv('S3_SECRET_ACCESS_KEY')
    s3 = boto3.client("s3", aws_access_key_id=S3_ACCESS_KEY_ID, aws_secret_access_key=S3_SECRET_ACCESS_KEY)
    try:
        yield s3
    finally:
        # No need to close the client for boto3
        pass