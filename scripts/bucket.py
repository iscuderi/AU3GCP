# -*- coding: utf-8 -*-
"""
Created on Wed May 25 14:27:46 2022

@author: ecpsc
"""

from google.cloud import storage

def create_bucket_class_location(bucket_name):
    """
    Create a new bucket in the US region with the coldline storage
    class
    """
    #bucket_name = "bucket_store1"

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    bucket.storage_class = "COLDLINE"
    new_bucket = storage_client.create_bucket(bucket, location="us")

    print(
        "Created bucket {} in {} with storage class {}".format(
            new_bucket.name, new_bucket.location, new_bucket.storage_class
        )
    )
    return new_bucket

create_bucket_class_location("bucket_store1")