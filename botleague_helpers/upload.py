from google.cloud import storage
from loguru import logger as log

AWS_DEEPDRIVE_BUCKET_NAME = 'deepdrive'
GCS_DEEPDRIVE_BUCKET_NAME = 'deepdriveio'

def upload_gcs(source_path: str, dest_path: str) -> str:
    log.info('Uploading %s to GCS bucket %s' % (source_path,
                                             GCS_DEEPDRIVE_BUCKET_NAME))
    key = dest_path
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(GCS_DEEPDRIVE_BUCKET_NAME)
    bucket.blob(key).upload_from_filename(source_path)
    url = f'https://storage.googleapis.com/{GCS_DEEPDRIVE_BUCKET_NAME}/{key}'
    log.info(f'Finished upload to {url}')
    return url


def upload_str(name: str, content: str, bucket_name: str):
    """

    :param name: Name of file including directories i.e. /my/path/file.txt
    :param content: UTF-8 encoded file content
    :param bucket_name: Name of GCS bucket, i.e. deepdriveio
    :return: Url of the public file
    """
    key = name
    bucket = storage.Client().get_bucket(bucket_name)
    blob = bucket.get_blob(key)
    blob = blob if blob is not None else bucket.blob(key)
    blob.upload_from_string(content)
    url = f'https://storage.googleapis.com/{bucket_name}/{key}'
    return url
