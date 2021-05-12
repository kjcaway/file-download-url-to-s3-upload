import boto3
import requests
import os
import traceback
from boto3.session import Session
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv(verbose=True)

AWS_ACCESS_KEY=os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY=os.getenv('AWS_SECRET_KEY')
AWS_CONFIG = Config(
    connect_timeout=3,
    read_timeout=3,
    retries={'max_attempts': 3},
)

def main():
    print("Start...")
    upload_from_url_to_s3("http://test.com") # pass file download url 
    print("End...")

def upload_from_url_to_s3(url):
    """
    request url(file download endpoint) and upload s3 without writing local storage.
    """
    try:
        session = requests.Session()
        res = session.get(
            url=url,
            stream=True
        )

        aws_session = Session(
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY
        )

        s3_resource = aws_session.resource(
            service_name='s3',
            config=AWS_CONFIG
        )
        s3_client = s3_resource.meta.client

        with res as part:
            part.raw.decode_content = True
            transfer_config = boto3.s3.transfer.TransferConfig(
                multipart_threshold=256,
                max_concurrency=4
            )
            s3_client.upload_fileobj(part.raw, 'bucket_name', 'file_name', Config=transfer_config)
            print("Success file upload to s3.")

            return True
    except Exception as e:
        traceback.print_exc()
        return False



if __name__ == "__main__":
    main()