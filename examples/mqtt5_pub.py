"""
SPDX-FileCopyrightText: Copyright (c) 2023 Contributors to the
Eclipse Foundation

See the NOTICE file(s) distributed with this work for additional
information regarding copyright ownership.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
SPDX-FileType: SOURCE
SPDX-License-Identifier: Apache-2.0
"""

import logging
import time

from google.protobuf.timestamp_pb2 import Timestamp
from uprotocol.communication.upayload import UPayload
from uprotocol.transport.builder.umessagebuilder import UMessageBuilder
from uprotocol.v1.umessage_pb2 import UMessage
from uprotocol.v1.uri_pb2 import UUri

from up_client_mqtt5_python.mqtt5_utransport import MQTT5UTransport

logging.basicConfig(format='%(levelname)s| %(filename)s:%(lineno)s %(message)s')
logger = logging.getLogger('File:Line# Debugger')
logger.setLevel(logging.DEBUG)


def build_source():
    return UUri(authority_name="vcu.matthew.com", ue_id=1234, ue_version_major=1, resource_id=0x8000)


def build_sink():
    return UUri(authority_name="vcu.matthew.com", ue_id=1234, ue_version_major=1, resource_id=0)


def build_timestamp_upayload():
    return UPayload.pack(Timestamp(seconds=1000, nanos=1000))


def build_umessage(payload, source=build_source()):
    return UMessageBuilder.publish(source=source).build_from_upayload(payload)


if __name__ == "__main__":
    mqtt5_publisher = MQTT5UTransport(build_sink(), "client_pub", "127.0.0.1", 1883, False)
    mqtt5_publisher.connect()
    umsg: UMessage = build_umessage(build_timestamp_upayload())
    while True:
        mqtt5_publisher.send(umsg)
        time.sleep(10)
