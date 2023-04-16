# coding: utf-8
# Copyright (c) 2016, 2023, Oracle and/or its affiliates.  All rights reserved.
# This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.

import oci
import sys
import time
import base64

PARTITIONS = 1
# DELETED
STREAM_NAME = "Stream_R1"
stream_OCID = "ocid1.stream.oc1.iad.amxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx2gjq"
stream_pool_id = "ocid1.streampool.oc1.iad.amaxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxodlq"

# ACTIVE
#STREAM_NAME = "Stream_R1"
#stream_OCID = "ocid1.stream.oc1.iad.amaaaaaaihuwreyat6d6dtnvm5nrnci56fusp2y543yzdhccrrwtxfxt2gjq"
#stream_pool_id = "ocid1.streampool.oc1.iad.amaaaaaaihuwreyaybybmrayawqkpa3z3o44cre752zp6u4zmkhoo34sybaq"


namespace = "ixxxxxxxxxbx"
bucket_name = "data"
object_name = "r1"
object_reverse = "r2"

def simple_check_loop(config, id):
    streaming_client = oci.streaming.StreamAdminClient(config)
    while True:
        try:
            list_streams_response = streaming_client.list_streams(stream_pool_id=id)
            token_status = list_streams_response.data[0].lifecycle_state
            print(token_status)
            if token_status == "ACTIVE":
                change_token(config, namespace, bucket_name, object_name, object_reverse)
            else:
                change_token(config, namespace, bucket_name, object_reverse, object_name)

        except:
            change_token(config, namespace, bucket_name, object_reverse, object_name)

def change_token(config, namespace, bucket_name, object_name, object_reverse):
    object_storage_client = oci.object_storage.ObjectStorageClient(config)
    put_object_response = object_storage_client.put_object(namespace, bucket_name, object_name, 'Test object content')

    try:
        object_storage_client.delete_object(namespace, bucket_name, object_reverse)
    except oci.exceptions.ServiceError as e:
        print('\nToken ' + object_reverse + ' not found\n')


# Load the default configuration
config = oci.config.from_file()
#config['log_requests'] = True

# A cursor can be created as part of a consumer group.
# Committed offsets are managed for the group, and partitions
# are dynamically balanced amongst consumers in the group.
#file1 = open("myfile.txt", "a")  # append mode
simple_check_loop(config, stream_pool_id)
