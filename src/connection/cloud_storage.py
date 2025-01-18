import os
import pytz
import time
from datetime import timedelta, datetime, date
import numpy  as np
import pandas as pd
import pandas_gbq
import pickle5 as pickle

from google.cloud import storage
from google.api_core.exceptions import NotFound

from src.connection.gcp_auth import GCPAuth
from src.config.helper import log_method_call


class GCSConn(GCPAuth):
    def __init__(self, bucket:str, scope=None):
        super().__init__(scope=scope)
        self.client = storage.Client(credentials=self.credential)
        self.bucket_name = bucket
        self.bucket = self.client.bucket(bucket)
    
    @log_method_call
    def upload_from_memory(self, contents, destination: str, content_type:str="text/plain"):
        blob = self.bucket.blob(destination)
        if content_type == "text/plain":
            target = pickle.dumps(contents) # serializing
        else:
            target = contents
        blob.upload_from_string(data=target, content_type=content_type)

    def upload_from_file(self, file, destination, content_type:str=None):
        # Optional: set a generation-match precondition to avoid potential race conditions
        # and data corruptions. The request to upload is aborted if the object's
        # generation number does not match your precondition. For a destination
        # object that does not yet exist, set the if_generation_match precondition to 0.
        # If the destination object already exists in your bucket, set instead a
        # generation-match precondition using its generation number.
        blob = self.bucket.blob(destination)
        generation_match_precondition = 0
        blob.upload_from_filename(filename=file, if_generation_match=generation_match_precondition, content_type=content_type)

        print(f"File {file} uploaded to {destination}.")


    def download_blob_into_memory(self, blob_name):
        """Downloads a blob into memory."""
        # Construct a client side representation of a blob.
        # Note `Bucket.blob` differs from `Bucket.get_blob` as it doesn't retrieve
        # any content from Google Cloud Storage. As we don't need additional data,
        # using `Bucket.blob` is preferred here.
        blob = self.bucket.blob(blob_name)
        contents = blob.download_as_bytes()
        return contents

    def download_blob_to_file(self, blob_name, destination_file_name):
        """Downloads a blob from the bucket."""
        # Construct a client side representation of a blob.
        # Note `Bucket.blob` differs from `Bucket.get_blob` as it doesn't retrieve
        # any content from Google Cloud Storage. As we don't need additional data,
        # using `Bucket.blob` is preferred here.
        blob = self.bucket.blob(blob_name)
        blob.download_to_filename(destination_file_name)