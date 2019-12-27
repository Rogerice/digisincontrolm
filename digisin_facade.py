import boto3
import os
from time import sleep

job1 = 'envioDocumentosDigisin'
fila_job1 = 'bb30-envio-documentos-digisin-backend-QUEUE-DEV'
def_job1 = 'bb30-envio-documentos-digisin-backend-JOBDEF-DEV'

job2 = 'webmethodoBatch'
fila_job2 = 'envio-documentos-digisin-webmethod-client-batch-QUEUE-DEV'
def_job2 = 'envio-documentos-digisin-webmethod-client-batch-JOBDEF-DEV'

profile_name = 'NPDev'

sucesso = 'SUCCE'
falha = 'FAIL'

buckets3 = 'dev-brasilseg-ged'

s3_to_local_map = [
    {'key': '/12', 'pasta_local': 'C:\digisin_12'},
    {'key': '/14', 'pasta_local': 'C:\digisin_14'}
]

boto3.setup_default_session(profile_name=profile_name)
client = boto3.client('batch')


def is_pode_continuar(jobId):
    continuar = True
    while continuar:
        sleep(5)
        job1Desc = client.describe_jobs(
            jobs=[jobId]
        )
        status = job1Desc['jobs'][0]['status']
        print(f'Status job: {jobId} = {status}')
        if sucesso in status:
            continuar = False
        if falha in status:
            raise Exception(f'Falha no job: {jobId}')


def start_aws_batch_sync(jobName, fila_job, def_job, ):
    print(f'Startando aws batch {def_job}')
    responseJob1 = client.submit_job(
        jobName=jobName,
        jobQueue=fila_job,
        jobDefinition=def_job
    )
    jobId = responseJob1['jobId']
    is_pode_continuar(jobId)
    print(f'Sucesso: {jobName}: {jobId}')


if __name__ == '__main__':
    start_aws_batch_sync(job1, fila_job1, def_job1)

    for map in s3_to_local_map:
        print(f'Executando copia do: {map} ')
        sync = f'aws s3 sync s3://{buckets3}{map["key"]} {map["pasta_local"]} --profile {profile_name}'
        print(sync)
        os.system(sync)

    start_aws_batch_sync(job2, fila_job2, def_job2)
