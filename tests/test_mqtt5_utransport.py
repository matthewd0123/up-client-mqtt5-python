# -------------------------------------------------------------------------

# Copyright (c) 2024 General Motors GTO LLC
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# SPDX-FileType: SOURCE
# SPDX-FileCopyrightText: 2024 General Motors GTO LLC
# SPDX-License-Identifier: Apache-2.0

# -------------------------------------------------------------------------

import unittest
import socket

from google.protobuf.timestamp_pb2 import Timestamp

from up_client_mqtt5_python.mqtt5_utransport import MQTT5UTransport
from uprotocol.transport.builder.uattributesbuilder import UAttributesBuilder
from uprotocol.transport.builder.upayloadbuilder import UPayloadBuilder
from uprotocol.uuid.factory.uuidfactory import Factories
from uprotocol.proto.uri_pb2 import UUri, UAuthority, UEntity
from uprotocol.uri.factory.uresource_builder import UResourceBuilder
from uprotocol.proto.ustatus_pb2 import UCode
from uprotocol.proto.umessage_pb2 import UMessage
from uprotocol.proto.uattributes_pb2 import UPriority
from uprotocol.transport.ulistener import UListener


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


def get_uuid():
    return Factories.UPROTOCOL.create()


def build_timestamp_upayload():
    return UPayloadBuilder.pack(Timestamp(seconds=1000, nanos=1000))


def build_publish_uattributes():
    source = build_source()
    return UAttributesBuilder.publish(source, UPriority.UPRIORITY_CS1).build()


def build_umessage():
    return UMessage(attributes=build_publish_uattributes(),
                    payload=build_timestamp_upayload())


class MQTT5UListener(UListener):
    def __init__(self) -> None:
        pass

    def on_receive(self, umsg: UMessage):
        pass


class TestMQTT5UTransport(unittest.TestCase):
    def setUp(self):
        self.transport = MQTT5UTransport(
            client_id="test_client", host_name="test_host", port=1883,
            cloud_device=True)

    def test_send_success(self):
        umsg = build_umessage()
        result = self.transport.send(umsg)
        self.assertEqual(result.code, UCode.OK)
        self.assertEqual(result.message, "OK")

    def test_register_listener_success(self):
        topic = build_source()
        listener = MQTT5UListener()
        result = self.transport.register_listener(topic, listener)
        self.assertEqual(result.code, UCode.OK)
        self.assertEqual(result.message, "OK")

    def test_unregister_listener_success(self):
        # Assuming UUri and UListener classes are defined
        topic = build_source()
        listener = MQTT5UListener()
        result = self.transport.unregister_listener(topic, listener)
        self.assertEqual(result.code, UCode.OK)
        self.assertEqual(result.message, "OK")


if __name__ == "__main__":
    unittest.main()
