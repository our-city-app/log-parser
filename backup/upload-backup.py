#!/usr/bin/env python3
# coding=utf-8
import logging
from sys import argv

from google.cloud import storage

logging.getLogger().setLevel(logging.DEBUG)


def upload_file(filename, destination, bucket):
    """
    Uploads a file to a given Cloud Storage bucket and returns the public url
    to the new object.
    """
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(destination)
    blob.upload_from_filename(filename)
    return blob.public_url


def main(backup_file):
    """
    Uploads the backup to google cloud storage
    """
    logging.info('Writing file %s to GCS.', backup_file)
    # Same file name every time. Versions will be used to remove old backups.
    dest = 'backups/log-parser.tar.gz'
    result = upload_file(backup_file, dest, 'gig-log-parser.appspot.com')
    logging.info('Uploaded result: %s', result)


if __name__ == '__main__':
    main(argv[1])
