import logging 
import json
from worker import s3, client, S3_BUCKET

logging.basicConfig(level=logging.INFO)

def lambda_handler(event, context):
    """lambda handler for processing SQS messages and storing them in S3"""
    logging.info(f"Received event: {json.dumps(event)}")
    
    for record in event.get("Records", []):
        body = json.loads(record["body"])
        
        client.put_object(Bucket=S3_BUCKET, Key=f"processed/{body.get('id', 'job')}.json", Body=json.dumps(body))
        
    return {
        "statusCode": 200,
        "body": json.dumps({"message": "Processed messages successfully"})
    }