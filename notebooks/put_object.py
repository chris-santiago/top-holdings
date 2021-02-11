# https://console.aws.amazon.com/console/home?region=us-east-1

import boto3
import os

# set params
aws_key = 'AKIA3NYBHVPWNTMCCVU5'
aws_secret = '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'
region = 'us-east-1'

# create s3 client
s3 = boto3.client(
    's3',
    region_name=region,
    aws_access_key_id=aws_key,
    aws_secret_access_key=aws_secret
)

bucket_name = 'chris-santiago.net'

for file in ['mega-cap.html', 'large-cap.html', 'mid-cap.html', 'small-cap.html', 'micro-cap.html']:
	key = f'plots/{file}'
	filename = file

	s3.upload_file(
	    Bucket=bucket_name,
	    Key=key,
	    Filename=filename
	)
