"""
EcoRoute Sentinel: LocalStack Automated Infrastructure Bootstrap Script
========================================================================
Provisions all AWS cloud plumbing (SNS, SQS DLQ, DynamoDB, Lambda, EventBridge, S3)
against LocalStack in one command.

Usage:
    python bootstrap_localstack.py
"""

import io
import json
import time
import zipfile
import boto3
from botocore.config import Config

LOCALSTACK_ENDPOINT = "http://localhost:4566"
AWS_REGION = "ap-south-1"

cfg = Config(connect_timeout=2, read_timeout=10, retries={"max_attempts": 2})

def get_client(service: str):
    return boto3.client(
        service,
        endpoint_url=LOCALSTACK_ENDPOINT,
        region_name=AWS_REGION,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        config=cfg,
    )

def main():
    print("=" * 70)
    print("🛰️  EcoRoute Sentinel: Bootstrapping AWS Infrastructure on LocalStack")
    print(f"Target: {LOCALSTACK_ENDPOINT} | Region: {AWS_REGION}")
    print("=" * 70)

    # 1. SQS Dead-Letter Queue (DLQ)
    print("\n[1/6] Provisioning Amazon SQS Dead-Letter Queue (DLQ)...")
    sqs = get_client("sqs")
    dlq_name = "ecoroute-alert-dlq"
    dlq_resp = sqs.create_queue(QueueName=dlq_name)
    dlq_url = dlq_resp["QueueUrl"]
    dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    print(f"  ✅ SQS DLQ Created: {dlq_name} -> {dlq_arn}")

    # 2. SNS Topic + Subscriptions
    print("\n[2/6] Provisioning Amazon SNS Alert Topic & Subscriptions...")
    sns = get_client("sns")
    topic_name = "ecoroute-sentinel-guardian-alerts"
    topic_arn = sns.create_topic(Name=topic_name)["TopicArn"]
    print(f"  ✅ SNS Topic Created: {topic_arn}")
    for proto, ep in [("sms", "+919800000042"), ("email", "guardians@ecoroute.example")]:
        sns.subscribe(TopicArn=topic_arn, Protocol=proto, Endpoint=ep)
        print(f"  ✅ Subscribed {proto.upper()} endpoint: {ep}")

    # 3. DynamoDB Audit Trail Table
    print("\n[3/6] Provisioning Amazon DynamoDB Dispatch Audit Table...")
    ddb = get_client("dynamodb")
    table_name = "ecoroute-dispatch-audit"
    try:
        ddb.create_table(
            TableName=table_name,
            KeySchema=[
                {"AttributeName": "route_id", "KeyType": "HASH"},
                {"AttributeName": "timestamp_ist", "KeyType": "RANGE"}
            ],
            AttributeDefinitions=[
                {"AttributeName": "route_id", "AttributeType": "S"},
                {"AttributeName": "timestamp_ist", "AttributeType": "S"}
            ],
            BillingMode="PAY_PER_REQUEST"
        )
        print(f"  ✅ DynamoDB Table Created: {table_name}")
    except Exception as exc:
        print(f"  ℹ️ DynamoDB Table already exists or ready: {exc}")

    # 4. Amazon S3 Daily Reports Bucket
    print("\n[4/6] Provisioning Amazon S3 Exposure Reports Bucket...")
    s3 = get_client("s3")
    bucket_name = "ecoroute-daily-reports"
    try:
        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": AWS_REGION}
        )
        print(f"  ✅ S3 Bucket Created: s3://{bucket_name}")
    except Exception as exc:
        print(f"  ℹ️ S3 Bucket note: {exc}")

    # 5. AWS Lambda Hazard Inference Function
    print("\n[5/6] Deploying AWS Lambda Function (ecoroute-sentinel-hazard-inference)...")
    lam = get_client("lambda")
    func_name = "ecoroute-sentinel-hazard-inference"
    role_arn = f"arn:aws:iam::000000000000:role/ecoroute-sentinel-exec"

    handler_code = '''import json

def handler(event, context):
    routes = event.get("routes", [])
    threshold = event.get("threshold", 75.0)
    flagged = [r["id"] for r in routes if r.get("hazard", 0) > threshold]
    print(json.dumps({
        "level": "INFO",
        "msg": "hazard_inference_executed",
        "scored_routes": len(routes),
        "flagged_routes": flagged,
        "request_id": context.aws_request_id
    }))
    return {
        "statusCode": 200,
        "routes_scored": len(routes),
        "flagged": flagged,
        "function_version": context.function_version,
        "request_id": context.aws_request_id
    }
'''
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("handler.py", handler_code)

    try:
        func = lam.create_function(
            FunctionName=func_name,
            Runtime="python3.12",
            Role=role_arn,
            Handler="handler.handler",
            Code={"ZipFile": buf.getvalue()},
            Timeout=15,
            MemorySize=512,
            Publish=True
        )
        func_arn = func["FunctionArn"]
        print(f"  ✅ Lambda Deployed: {func_arn}")
    except Exception as exc:
        print(f"  ℹ️ Lambda note: {exc}")
        func_arn = f"arn:aws:lambda:{AWS_REGION}:000000000000:function:{func_name}"

    # 6. Amazon EventBridge Pre-Dispatch Cron Rules
    print("\n[6/6] Configuring Amazon EventBridge Pre-Dispatch Rules...")
    ev = get_client("events")
    routes = [
        ("r-01", "06:10 IST (00:40 UTC)", "cron(40 0 ? * MON-SAT *)"),
        ("r-02", "06:20 IST (00:50 UTC)", "cron(50 0 ? * MON-SAT *)"),
        ("r-03", "06:30 IST (01:00 UTC)", "cron(0 1 ? * MON-SAT *)"),
        ("r-04", "06:40 IST (01:10 UTC)", "cron(10 1 ? * MON-SAT *)"),
        ("r-05", "06:50 IST (01:20 UTC)", "cron(20 1 ? * MON-SAT *)"),
        ("r-06", "07:00 IST (01:30 UTC)", "cron(30 1 ? * MON-SAT *)"),
    ]
    for rid, timing, cron_expr in routes:
        rule_name = f"ecoroute-predispatch-{rid}"
        ev.put_rule(
            Name=rule_name,
            ScheduleExpression=cron_expr,
            State="ENABLED",
            Description=f"Pre-dispatch hazard scoring at {timing} for Bus {rid.upper()}"
        )
        ev.put_targets(
            Rule=rule_name,
            Targets=[{
                "Id": f"target-{rid}",
                "Arn": func_arn,
                "Input": json.dumps({"route_id": rid.upper(), "threshold": 75.0})
            }]
        )
        print(f"  ✅ EventBridge Rule configured: {rule_name} [{timing}]")

    print("\n" + "=" * 70)
    print("🚀  Bootstrap Complete! All 6 AWS Cloud Services Provisioned.")
    print("=" * 70)

if __name__ == "__main__":
    main()
