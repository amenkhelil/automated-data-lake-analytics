# -*- coding: utf-8 -*-
"""
Handler Lambda pour l'orchestration autonome du Data Lake & Data Mesh.
- Déclenchement S3 (ObjectCreated dans raw/*.csv) :
    1. Inspecte les en-têtes du fichier déposé directement depuis S3.
    2. Exécute l'Agent Data Mesh pour classifier le domaine (Ventes, Finance, RH...).
    3. Génère et valide dynamiquement le script PySpark 'active_etl.py' avec gouvernance et Iceberg.
    4. Met à jour le script actif dans s3://<bucket>/scripts/active_etl.py.
    5. Démarre le Job AWS Glue ETL adapté au domaine détecté.
- Déclenchement EventBridge (Glue Job SUCCEEDED) :
    Démarre automatiquement le Crawler Glue pour mettre à jour le Data Catalog.
"""
import csv
import json
import os
import urllib.parse
import boto3

from data_mesh_agent import DataMeshAgent

s3_client = boto3.client('s3')
glue_client = boto3.client('glue')

GLUE_JOB_NAME = os.environ.get('GLUE_JOB_NAME', 'orders-etl-job')
GLUE_CRAWLER_NAME = os.environ.get('GLUE_CRAWLER_NAME', 'orders-processed-crawler')


def lambda_handler(event, context):
    print("Received event:", json.dumps(event))

    # Cas 1 : Déclenchement par Amazon S3 (Nouveau fichier brut uploadé dans raw/)
    if 'Records' in event and len(event['Records']) > 0 and 's3' in event['Records'][0]:
        record = event['Records'][0]
        bucket_name = record['s3']['bucket']['name']
        object_key = urllib.parse.unquote_plus(record['s3']['object']['key'])

        print(f"[ORCHESTRATEUR] Objet S3 détecté : s3://{bucket_name}/{object_key}")

        if object_key.startswith('raw/') and object_key.endswith('.csv'):
            print(f"[DATA MESH AGENT] Inspection des en-têtes pour : {object_key}")

            # 1. Lecture de l'échantillon initial (4 KB) depuis S3
            try:
                response = s3_client.get_object(Bucket=bucket_name, Key=object_key, Range='bytes=0-4096')
                content_sample = response['Body'].read().decode('utf-8', errors='ignore')
                lines = [line for line in content_sample.splitlines() if line.strip()]
                if lines:
                    reader = csv.reader([lines[0]])
                    headers = next(reader, [])
                else:
                    headers = []
            except Exception as exc:
                print(f"[ERREUR] Échec de lecture de l'échantillon S3 : {exc}")
                headers = []

            # 2. Classification et génération du script actif par l'Agent
            agent_result = DataMeshAgent.generate_script_from_headers(headers, source_name=os.path.basename(object_key))
            domain = agent_result.get('domain', 'GENERAL_DATA')
            confidence = agent_result.get('confidence', 0.0)
            status = agent_result.get('status', 'OK')

            print(f"[DATA MESH AGENT] Domaine identifié : {domain} (Confiance : {confidence}%) — Statut : {status}")

            if status == "REVIEW_REQUIRED":
                print(f"[SÉCURITÉ] Score de confiance trop faible ({confidence}% < 60%). Révision requise.")
                return {
                    'statusCode': 200,
                    'body': json.dumps({'action': 'REVIEW_REQUIRED', 'domain': domain, 'confidence': confidence})
                }

            # 3. Synchronisation du script PySpark actif sur S3
            script_content = agent_result.get('script_content')
            if script_content:
                s3_client.put_object(
                    Bucket=bucket_name,
                    Key='scripts/active_etl.py',
                    Body=script_content.encode('utf-8')
                )
                print(f"[STOCKAGE] Script actif s3://{bucket_name}/scripts/active_etl.py synchronisé pour {domain}.")

            # 4. Lancement du Job AWS Glue avec ciblage du fichier source
            print(f"[ETL] Démarrage du job Glue : {GLUE_JOB_NAME} pour {object_key}")
            glue_response = glue_client.start_job_run(
                JobName=GLUE_JOB_NAME,
                Arguments={
                    '--S3_BUCKET_NAME': bucket_name,
                    '--SOURCE_KEY':     object_key
                }
            )
            job_run_id = glue_response.get('JobRunId')
            print(f"[ETL] Job Glue démarré avec succès. JobRunId : {job_run_id}")

            return {
                'statusCode': 200,
                'body': json.dumps({
                    'action': 'GLUE_JOB_STARTED',
                    'jobRunId': job_run_id,
                    'domain': domain,
                    'confidence': confidence
                })
            }
        else:
            print(f"[IGNORÉ] L'objet ne correspond pas à raw/*.csv : {object_key}")
            return {
                'statusCode': 200,
                'body': json.dumps({'action': 'IGNORED', 'file': object_key})
            }

    # Cas 2 : Déclenchement par Amazon EventBridge (Fin avec succès du Job Glue)
    elif event.get('source') == 'aws.glue' and event.get('detail', {}).get('state') == 'SUCCEEDED':
        job_name = event['detail'].get('jobName')
        print(f"[EVENTBRIDGE] Le job Glue '{job_name}' a terminé avec statut SUCCEEDED.")
        print(f"[CRAWLER] Démarrage du crawler Glue : {GLUE_CRAWLER_NAME}")

        try:
            glue_client.start_crawler(Name=GLUE_CRAWLER_NAME)
            print(f"[CRAWLER] Crawler '{GLUE_CRAWLER_NAME}' démarré.")
            return {
                'statusCode': 200,
                'body': json.dumps({'action': 'CRAWLER_STARTED'})
            }
        except glue_client.exceptions.CrawlerRunningException:
            print(f"[CRAWLER] Le crawler '{GLUE_CRAWLER_NAME}' est déjà en cours d'exécution.")
            return {
                'statusCode': 200,
                'body': json.dumps({'action': 'CRAWLER_ALREADY_RUNNING'})
            }

    print("[INFO] Aucun traitement requis pour cet événement.")
    return {
        'statusCode': 200,
        'body': json.dumps({'action': 'NO_OP'})
    }
