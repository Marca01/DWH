import os
import threading
from dotenv import load_dotenv
from google.cloud import storage

load_dotenv()

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'ServiceKey_GoogleCloudStorage.json'

project_id = os.getenv('PROJECT_ID')

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print(
        f"File {source_file_name} uploaded to {destination_blob_name}."
    )
# def upload_blob_from_memory(bucket_name, contents, destination_blob_name):
#     storage_client = storage.Client()
#     bucket = storage_client.bucket(bucket_name)
#     blob = bucket.blob(destination_blob_name)
#
#     # blob.upload_from_string(contents)
#     blob.upload_from_string(contents, content_type='application/json')
#
#     print(
#         f"{destination_blob_name} uploaded to {bucket_name}."
#     )

bucket_name = os.getenv('BUCKET_NAME')

airflow_bucket_name = 'asia-southeast1-epl-airflow-6dabbd36-bucket'
def uploadAirflowFile():
    destination_blob_name = f"dags/local_dag.py"
    # destination_blob_name = f"dags/main.py"
    # destination_blob_name = f"dags/etl.py"
    # destination_blob_name = f"dags/utilss.py"
    # destination_blob_name = f"pyspark/.env"
    # destination_blob_name = f"pyspark/ServiceKey_GoogleCloudStorage.json"

    source_file_name = os.path.join(os.path.dirname(__file__), "dags/local_dag.py")
    # source_file_name = os.path.join(os.path.dirname(__file__), "main.py")
    # source_file_name = os.path.join(os.path.dirname(__file__), "etl.py")
    # source_file_name = os.path.join(os.path.dirname(__file__), "utilss.py")
    # source_file_name = os.path.join(os.path.dirname(__file__), ".env")
    # source_file_name = os.path.join(os.path.dirname(__file__), "ServiceKey_GoogleCloudStorage.json")

    # upload_blob(airflow_bucket_name, source_file_name, destination_blob_name)
    upload_blob(airflow_bucket_name, source_file_name, destination_blob_name)

if __name__ == '__main__':
    # start = time.time()
    # uploadAirflowFile()
    # print('helloo')
    airflow_file_thread = threading.Thread(target=uploadAirflowFile)

    airflow_file_thread.start()

    airflow_file_thread.join()

    # end = time.time()
    # print('Execution Time: {}'.format(end - start))