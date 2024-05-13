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
import socket

from up_client_mqtt5_python.mqtt5_utransport import MQTT5UTransport

from google.protobuf.timestamp_pb2 import Timestamp

from uprotocol.proto.umessage_pb2 import UMessage
from uprotocol.proto.uattributes_pb2 import UPriority
from uprotocol.proto.uri_pb2 import UUri, UAuthority, UEntity
from uprotocol.transport.builder.uattributesbuilder import UAttributesBuilder
from uprotocol.transport.builder.upayloadbuilder import UPayloadBuilder
from uprotocol.uri.factory.uresource_builder import UResourceBuilder

logging.basicConfig(
    format='%(levelname)s| %(filename)s:%(lineno)s %(message)s')
logger = logging.getLogger('File:Line# Debugger')
logger.setLevel(logging.DEBUG)


def build_source():
    return UUri(authority=UAuthority(name="vcu.someVin.veh.ultifi.gm.com",
                                     ip=bytes(socket.inet_pton(
                                         socket.AF_INET, "10.0.3.3"))),
                entity=UEntity(name="petapp.ultifi.gm.com",
                               version_major=1, id=1234),
                resource=UResourceBuilder.for_rpc_request(None))


def build_sink():
    return UUri(authority=UAuthority(name="vcu.someVin.veh.ultifi.gm.com",
                                     ip=bytes(socket.inet_pton(
                                         socket.AF_INET, "10.0.3.3"))),
                entity=UEntity(name="petapp.ultifi.gm.com",
                               version_major=1, id=1234),
                resource=UResourceBuilder.for_rpc_response())


def build_timestamp_upayload():
    return UPayloadBuilder.pack(Timestamp(seconds=1000, nanos=1000))


def build_publish_uattributes():
    source = build_source()
    return UAttributesBuilder.publish(source, UPriority.UPRIORITY_CS1
                                      ).withSink(build_sink()).build()


def build_umessage():
    return UMessage(attributes=build_publish_uattributes(),
                    payload=build_timestamp_upayload())


if __name__ == "__main__":
    mqtt5_publisher = MQTT5UTransport("client_pub", "127.0.0.1", 1883, True)
    mqtt5_publisher.connect()
    umsg: UMessage = build_umessage()
    while True:
        mqtt5_publisher.send(umsg)
        time.sleep(10)
