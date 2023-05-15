# coding: utf-8
# Copyright (c) 2016, 2023, Oracle and/or its affiliates.  All rights reserved.
# This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.

import oci
import sys
import time
import base64
from datetime import datetime

# Documentation : https://docs.cloud.oracle.com/iaas/Content/Streaming/Concepts/streamingoverview.htm
# Usage : python stream_example.py 
PARTITIONS = 1

STREAM_NAME = "Stream_API"
stream_OCID = "ocid1.stream.oc1.iad.am.............................................wna"

#message_Endpoint = "https://pwqvog4j723sutbgaxb2zbmv2q.apigateway.us-ashburn-1.oci.customer-oci.com"
message_Endpoint = "https://cell-1.streaming.us-ashburn-1.oci.oraclecloud.com"

is_DR = True
namespace = "i............x"
bucket_name = "data"
object_name = "r1"
object_reverse = "r2"

return_limit = 10000

def simple_message_loop(config, namespace, bucket_name, object_name, object_reverse, is_DR, client, stream_id, return_limit):
    stream_timestamp = get_timestamp()
    flag_cursor = False
    while True:
        if check_token(config, namespace, bucket_name, object_name, object_reverse, is_DR):
            if not flag_cursor:
                cursor = get_cursor_by_group(stream_client, s_id, "example-group", "example-instance-1", stream_timestamp)
                flag_cursor = True

            get_response = client.get_messages(stream_id, cursor)
            # No messages to process. return.
            if not get_response.data:
                print("Waiting for message in R2...")

                ##return

            # Process the messages
            print(" Read {} messages".format(len(get_response.data)))

            for message in get_response.data:
                decoded_bytes = base64.b64decode(message.value)
                decoded_string = decoded_bytes.decode("utf-8")
                #validate if token is active
                print("[MESSAGE] "+decoded_string)

            # get_messages is a throttled method; clients should retrieve sufficiently large message
            # batches, as to avoid too many http requests.
            time.sleep(1)
            # use the next-cursor for iteration
            cursor = get_response.headers["opc-next-cursor"]
        else:
            stream_timestamp = get_timestamp()
            flag_cursor = False

def get_timestamp():
    today = datetime.now() - timedelta(hours=0, minutes=5)
    iso_date = today.strftime("%Y-%m-%dT%H:%M:%SZ")
    return iso_date

def check_token(config, namespace, bucket_name, object_name, object_reverse, is_DR):
    object_storage_client = oci.object_storage.ObjectStorageClient(config)
    try:
        if is_DR:
            object_storage_client.get_object(namespace, bucket_name, object_reverse)
            return True
        else:
            object_storage_client.get_object(namespace, bucket_name, object_name)
            return True
    except oci.exceptions.ServiceError as e:
        return False


def get_cursor_by_group(sc, sid, group_name, instance_name, stream_timestamp=None):
    print(" Creating a cursor for group {}, instance {}".format(group_name, instance_name))
    if stream_timestamp is None:
        cursor_details = oci.streaming.models.CreateGroupCursorDetails(group_name=group_name, instance_name=instance_name,
                                                                   type=oci.streaming.models.
                                                                   CreateGroupCursorDetails.TYPE_TRIM_HORIZON,
                                                                   commit_on_get=True)
    else:
        cursor_details = oci.streaming.models.CreateGroupCursorDetails(group_name=group_name, instance_name=instance_name,
                                                                       type=oci.streaming.models.
                                                                       CreateGroupCursorDetails.TYPE_AT_TIME,
                                                                       time=stream_timestamp,
                                                                       commit_on_get=True)
    response = sc.create_group_cursor(sid, cursor_details)
    return response.data.value


# Load the default configuration
config = oci.config.from_file()
#config['log_requests'] = True

# Create a StreamAdminClientCompositeOperations for composite operations.
stream_admin_client = oci.streaming.StreamAdminClient(config)
stream_admin_client_composite = oci.streaming.StreamAdminClientCompositeOperations(stream_admin_client)
 
print("Using limit of retrieve: "+ str(return_limit))

# Streams are assigned a specific endpoint url based on where they are provisioned.
# Create a stream client using the provided message endpoint.
stream_client = oci.streaming.StreamClient(config, service_endpoint=message_Endpoint)
s_id = stream_OCID

simple_message_loop(config, namespace, bucket_name, object_name, object_reverse, is_DR, stream_client, s_id, return_limit)
