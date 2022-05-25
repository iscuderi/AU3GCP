# -*- coding: utf-8 -*-
"""
Created on Wed May 25 12:37:58 2022

@author: ecpsc
"""

from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
import threading


project_id = "watchful-bonus-345012"
#timeout = 60*5

# Number of seconds the subscriber should listen for messages

from google.cloud import storage

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    #bucket_name = "bucket_store1"
    # The path to your file to upload
    #source_file_name = "bucketstore1"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        "File {} uploaded to {}.".format(
            source_file_name, destination_blob_name
        )
    )

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    print(f"Received {message}.")
    
    message.ack()
    file_buy=open("file_buy.txt","a")
    file_search=open("file_search.txt","a")
    file_book=open("file_book.txt","a")

    try:
        topic,data=message.data.decode("utf-8").split(";")
        if topic=="buy":
            file_buy.write(data+"\n")
        if topic=="search":
            file_search.write(data+"\n")
        if topic=="book":
            file_book.write(data+"\n")
        
        file_buy.close()
        file_book.close()
        file_search.close()
    
    except:
        pass
    upload_blob("bucket_store1", "file_buy.txt","file_buy.txt")
    upload_blob("bucket_store1", "file_search.txt", "file_search.txt")
    upload_blob("bucket_store1","file_book.txt","file_book.txt")
    
        
subscription_id1 = "suscbuy"
subscription_id2 = "suscbook"
subscription_id3 = "suscsearch"

subscriber1 = pubsub_v1.SubscriberClient()
subscriber2 = pubsub_v1.SubscriberClient()
subscriber3 = pubsub_v1.SubscriberClient()

subscription_path1 = subscriber1.subscription_path(project_id, subscription_id1)
subscription_path2 = subscriber2.subscription_path(project_id, subscription_id2)
subscription_path3 = subscriber2.subscription_path(project_id, subscription_id3)


streaming_pull_future1 = subscriber1.subscribe(subscription_path1, callback=callback)
print(f"Listening for messages on {subscription_path1}..\n")
streaming_pull_future2 = subscriber2.subscribe(subscription_path2, callback=callback)
print(f"Listening for messages on {subscription_path2}..\n")
streaming_pull_future3 = subscriber3.subscribe(subscription_path3, callback=callback)
print(f"Listening for messages on {subscription_path3}..\n")


subscriber_shutdown = threading.Event()
streaming_pull_future1.add_done_callback(lambda result: subscriber_shutdown.set())
streaming_pull_future2.add_done_callback(lambda result: subscriber_shutdown.set())
streaming_pull_future3.add_done_callback(lambda result: subscriber_shutdown.set())


with subscriber1, subscriber2, subscriber3:

        subscriber_shutdown.wait()
        streaming_pull_future1.cancel()  # Trigger the shutdown.
        streaming_pull_future1.result()  # Block until the shutdown is complete.
    
        streaming_pull_future2.cancel()  # Trigger the shutdown.
        streaming_pull_future2.result()  # Block until the shutdown is complete.
        
        streaming_pull_future3.cancel()  # Trigger the shutdown.
        streaming_pull_future3.result()
        
        