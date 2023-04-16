# coding: utf-8
# Copyright (c) 2016, 2023, Oracle and/or its affiliates.  All rights reserved.
# This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at 
#http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.

import oci
import sys
import time
from base64 import b64encode, b64decode

# ==========================================================
# This file provides an example of basic streaming usage
# * - List streams
# * - Get a Stream
# * - Create a Stream
# * - Delete a Stream
# * - Publish to a Stream
# * - Consume from a stream partition using cursor
# * - Consume from a stream using a group cursor
# Documentation : https://docs.cloud.oracle.com/iaas/Content/Streaming/Concepts/streamingoverview.htm

# Usage : python stream_example.py <compartment id>

STREAM_NAME_R1 = "Stream_API"
STREAM_NAME_R2 = "Stream_R1"
PARTITIONS = 1
compartment = "ocid1.compartment.oc1..aaaxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxnzq"

def publish_example_messages(client_r1, stream_id_r1, client_r2, stream_id_r2):
    # Build up a PutMessagesDetails and publish some messages to the stream
    message_list = []
    i = 0
    while True:
        key = "key" + str(i)
        value = "value" + str(i)
        encoded_key = b64encode(key.encode()).decode()
        encoded_value = b64encode(value.encode()).decode()
        message_list.append(oci.streaming.models.PutMessagesDetailsEntry(key=encoded_key, value=encoded_value))

        messages = oci.streaming.models.PutMessagesDetails(messages=message_list)
        put_message_result = client_r1.put_messages(stream_id_r1, messages)
        put_message_result = client_r2.put_messages(stream_id_r2, messages)
        i=i+1
        message_list.clear

def get_or_create_stream(client, compartment_id, stream_name, partition, sac_composite):

    list_streams = client.list_streams(compartment_id=compartment_id, name=stream_name,
                                       lifecycle_state=oci.streaming.models.StreamSummary.LIFECYCLE_STATE_ACTIVE)
    if list_streams.data:
        # If we find an active stream with the correct name, we'll use it.
        print("An active stream {} has been found".format(stream_name))
        sid = list_streams.data[0].id
        return get_stream(sac_composite.client, sid)

    print(" No Active stream  {} has been found; Creating it now. ".format(stream_name))
    print(" Creating stream {} with {} partitions.".format(stream_name, partition))

    # Create stream_details object that need to be passed while creating stream.
    stream_details = oci.streaming.models.CreateStreamDetails(name=stream_name, partitions=partition,
                                                              compartment_id=compartment, retention_in_hours=24)

    # Since stream creation is asynchronous; we need to wait for the stream to become active.
    response = sac_composite.create_stream_and_wait_for_state(
        stream_details, wait_for_states=[oci.streaming.models.StreamSummary.LIFECYCLE_STATE_ACTIVE])
    return response


def get_stream(admin_client, stream_id):
    return admin_client.get_stream(stream_id)


# Load the default configuration
config = oci.config.from_file()

# Create a StreamAdminClientCompositeOperations for composite operations.
stream_admin_client = oci.streaming.StreamAdminClient(config)
stream_admin_client_composite = oci.streaming.StreamAdminClientCompositeOperations(stream_admin_client)

# Region 1
stream_r1 = get_or_create_stream(stream_admin_client, compartment, STREAM_NAME_R1,
                              PARTITIONS, stream_admin_client_composite).data
stream_client_r1 = oci.streaming.StreamClient(config, service_endpoint=stream_r1.messages_endpoint)
s_id_r1 = stream_r1.id

# Region 2
stream_r2 = get_or_create_stream(stream_admin_client, compartment, STREAM_NAME_R2,
                                 PARTITIONS, stream_admin_client_composite).data
stream_client_r2 = oci.streaming.StreamClient(config, service_endpoint=stream_r2.messages_endpoint)
s_id_r2 = stream_r2.id

# Publish some messages to the stream
publish_example_messages(stream_client_r1, s_id_r1, stream_client_r2, s_id_r2)

