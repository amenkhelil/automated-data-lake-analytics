"""
Handler Lambda pour l'orchestration événementielle du Data Lake.
- Déclenchement S3 (ObjectCreated) -> Démarre le Job Glue ETL.
- Déclenchement EventBridge (Glue Job SUCCEEDED) -> Démarre le Crawler Glue.
"""
import json
import os
import urllib.parse
import boto3

glue_client = boto3.client('glue')

GLUE_JOB_NAME = os.environ.get('GLUE_JOB_NAME', 'orders-etl-job')
GLUE_CRAWLER_NAME = os.environ.get('GLUE_CRAWLER_NAME', 'orders-processed-crawler')

def lambda_handler(event, context):
    print("Received event:", json.dumps(event))

    # Cas 1 : Déclenchement par Amazon S3 (Nouveau fichier brut uploadé)
    if 'Records' in event and len(event['Records']) > 0 and 's3' in event['Records'][0]:
        record = event['Records'][0]
        bucket_name = record['s3']['bucket']['name']
        object_key = urllib.parse.unquote_plus(record['s3']['object']['key'])

        print(f"S3 Object detected: s3://{bucket_name}/{object_key}")

        if object_key.startswith('raw/') and object_key.endswith('.csv'):
            print(f"Starting Glue job: {GLUE_JOB_NAME}")
            response = glue_client.start_job_run(
                JobName=GLUE_JOB_NAME,
                Arguments={
                    '--S3_BUCKET_NAME': bucket_name
                }
            )
            job_run_id = response.get('JobRunId')
            print(f"Glue job started successfully. JobRunId: {job_run_id}")
            return {
                'statusCode': 200,
                'body': json.dumps({'action': 'GLUE_JOB_STARTED', 'jobRunId': job_run_id})
            }
        else:
            print(f"Object ignored (does not match raw/*.csv): {object_key}")
            return {
                'statusCode': 200,
                'body': json.dumps({'action': 'IGNORED', 'file': object_key})
            }

    # Cas 2 : Déclenchement par Amazon EventBridge (Fin avec succès du Job Glue)
    elif event.get('source') == 'aws.glue' and event.get('detail', {}).get('state') == 'SUCCEEDED':
        job_name = event['detail'].get('jobName')
        print(f"Glue job '{job_name}' completed with status SUCCEEDED.")
        print(f"Starting Glue crawler: {GLUE_CRAWLER_NAME}")

        try:
            glue_client.start_crawler(Name=GLUE_CRAWLER_NAME)
            print(f"Glue crawler '{GLUE_CRAWLER_NAME}' started.")
            return {
                'statusCode': 200,
                'body': json.dumps({'action': 'CRAWLER_STARTED'})
            }
        except glue_client.exceptions.CrawlerRunningException:
            print(f"Glue crawler '{GLUE_CRAWLER_NAME}' is already running.")
            return {
                'statusCode': 200,
                'body': json.dumps({'action': 'CRAWLER_ALREADY_RUNNING'})
            }

    print("[INFO] Aucun traitement requis pour cet événement.")
    return {
        'statusCode': 200,
        'body': json.dumps({'action': 'NO_OP'})
    }
