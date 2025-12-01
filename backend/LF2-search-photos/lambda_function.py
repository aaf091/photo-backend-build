import json
import boto3
import os
import urllib3
from botocore.awsrequest import AWSRequest
from botocore.auth import SigV4Auth

session = boto3.Session()
credentials = session.get_credentials()
region = session.region_name or "us-east-1"

http = urllib3.PoolManager()
ES_ENDPOINT = os.environ["ES_ENDPOINT"]
INDEX = "photos"


# ---------------------------
# SIGN OPENSEARCH REQUESTS
# ---------------------------
def sign_request(method, url, body):
    req = AWSRequest(method=method, url=url, data=body)
    req.headers["Content-Type"] = "application/json"
    SigV4Auth(credentials, "es", region).add_auth(req)
    return dict(req.headers.items())


# ---------------------------
# SEARCH IN OPENSEARCH
# ---------------------------
def search_es(keyword):
    url = f"{ES_ENDPOINT}/{INDEX}/_search"
    query = {
        "size": 50,
        "query": {
            "match": { "labels": keyword }
        }
    }
    body = json.dumps(query).encode()

    headers = sign_request("GET", url, body)
    response = http.request("GET", url, body=body, headers=headers)

    return json.loads(response.data.decode())


# ---------------------------
# FORMAT RESULTS FOR FRONTEND
# ---------------------------
def format_api_results(keyword, hits):
    results = [
        {
            "objectKey": hit["_source"]["objectKey"],
            "bucket": hit["_source"]["bucket"],
            "labels": hit["_source"]["labels"],
            "url": f"https://{hit['_source']['bucket']}.s3.amazonaws.com/{hit['_source']['objectKey']}"
        }
        for hit in hits["hits"]["hits"]
    ]

    return {
        "query": keyword,
        "count": len(results),
        "results": results
    }


# ---------------------------
# FORMAT MESSAGE FOR LEX
# ---------------------------
def format_lex_message(keyword, hits):
    total = hits["hits"]["total"]["value"]

    if total == 0:
        return f"No photos found for '{keyword}'."

    keys = [hit["_source"]["objectKey"] for hit in hits["hits"]["hits"]]
    return f"I found {total} photos for {keyword}: {', '.join(keys)}"


# ---------------------------
# LEX HANDLER
# ---------------------------
def handle_lex(event):
    keyword = event["sessionState"]["intent"]["slots"]["Keyword"]["value"]["interpretedValue"]

    hits = search_es(keyword)
    msg = format_lex_message(keyword, hits)

    return {
        "sessionState": {
            "dialogAction": {"type": "Close"},
            "intent": {
                "name": "SearchIntent",
                "state": "Fulfilled"
            }
        },
        "messages": [
            {"contentType": "PlainText", "content": msg}
        ]
    }


# ---------------------------
# API GATEWAY HANDLER (WITH FULL CORS)
# ---------------------------
def handle_api(event):
    try:
        keyword = event["queryStringParameters"]["q"]
    except:
        return build_response(400, {"error": "Missing query parameter q"})

    hits = search_es(keyword)
    response_body = format_api_results(keyword, hits)

    return build_response(200, response_body)


# ---------------------------
# STANDARDIZED CORS RESPONSE
# ---------------------------
def build_response(status, body_dict):
    return {
        "statusCode": status,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
            "Access-Control-Allow-Methods": "GET,OPTIONS",
            "Content-Type": "application/json"
        },
        "body": json.dumps(body_dict)
    }


# ---------------------------
# MAIN HANDLER
# ---------------------------
def lambda_handler(event, context):
    print("EVENT:", json.dumps(event))

    # Lex V2 event
    if "sessionState" in event:
        return handle_lex(event)

    # API Gateway proxy event
    if "httpMethod" in event:
        return handle_api(event)

    return build_response(400, {"error": "Unknown request source"})