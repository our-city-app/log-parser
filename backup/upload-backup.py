#!/usr/bin/env python3
# coding=utf-8
import logging
import os
import time
from sys import argv

from google.cloud import storage

logging.getLogger().setLevel(logging.DEBUG)

CHUNK_SIZE = 104857600  # 100MB


def upload_file(filename, destination, bucket):
    """
    Uploads a file to a given Cloud Storage bucket and returns the public url
    to the new object.
    """
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(destination, chunk_size=CHUNK_SIZE)
    blob.upload_from_filename(filename)
    return blob.public_url


def main(root_dir):
    """
    Uploads the backup to google cloud storage
    """
    folder = time.strftime('%Y-%m-%d %H:%M')
    logging.info('Backing up folder %s', root_dir)
    for subdir, dirs, files in os.walk(root_dir):
        for filename in files:
            path = os.path.join(subdir, filename)
            dest = folder + path
            logging.info('Writing file %s to GCS as %s', path, dest)
            result = upload_file(path, dest, 'gig-log-parser.appspot.com')
            logging.info('Uploaded result: %s', result)


if __name__ == '__main__':
    main(argv[1])
