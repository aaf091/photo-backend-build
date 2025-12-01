import json
import boto3
import os
from datetime import datetime
from botocore.awsrequest import AWSRequest
from botocore.auth import SigV4Auth
import urllib3

s3 = boto3.client("s3")
rekognition = boto3.client("rekognition")

# OpenSearch endpoint from Lambda environment variable
ES_ENDPOINT = os.environ["ES_ENDPOINT"]

# For SigV4 signing
session = boto3.Session()
credentials = session.get_credentials()
region = session.region_name or "us-east-1"
service = "es"

http = urllib3.PoolManager()

def sign_request(method, url, body):
    # Add content-type before signing
    request = AWSRequest(method=method, url=url, data=body)
    request.headers["Content-Type"] = "application/json"

    # Sign request
    SigV4Auth(credentials, service, region).add_auth(request)

    # Convert signed headers to dictionary
    headers = dict(request.headers.items())

    response = http.request(
        method,
        url,
        body=body,
        headers=headers
    )
    return response


def lambda_handler(event, context):
    print("Event:", json.dumps(event))

    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]

        # ---------------------------
        # 1. Extract metadata (custom labels)
        # ---------------------------
        head = s3.head_object(Bucket=bucket, Key=key)
        metadata = head.get("Metadata", {})

        custom_labels_str = metadata.get("customlabels", "")
        custom_labels = [lbl.strip().lower()
                         for lbl in custom_labels_str.split(",")
                         if lbl.strip()]

        # ---------------------------
        # 2. Run Rekognition
        # ---------------------------
        rekog_resp = rekognition.detect_labels(
            Image={"S3Object": {"Bucket": bucket, "Name": key}},
            MaxLabels=10,
            MinConfidence=75
        )
        rekog_labels = [lbl["Name"].lower() for lbl in rekog_resp["Labels"]]

        # ---------------------------
        # 3. Combine labels
        # ---------------------------
        labels = list(set(rekog_labels + custom_labels))

        # ---------------------------
        # 4. Build document
        # ---------------------------
        created_timestamp = head["LastModified"].isoformat()

        doc = {
            "objectKey": key,
            "bucket": bucket,
            "createdTimestamp": created_timestamp,
            "labels": labels
        }

        print("Doc to index:", json.dumps(doc))

        # ---------------------------
        # 5. Index into OpenSearch
        # ---------------------------
        index = "photos"
        url = f"{ES_ENDPOINT}/{index}/_doc/{key}"

        body = json.dumps(doc).encode("utf-8")

        response = sign_request("PUT", url, body)
        print("OpenSearch Response status:", response.status)
        print("OpenSearch Response body:", response.data.decode("utf-8"))

    return {"statusCode": 200, "body": "OK"}