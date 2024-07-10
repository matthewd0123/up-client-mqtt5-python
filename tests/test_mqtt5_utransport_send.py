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

import json
import logging

from google.protobuf.timestamp_pb2 import Timestamp
from uprotocol.communication.upayload import UPayload
from uprotocol.transport.builder.umessagebuilder import UMessageBuilder
from uprotocol.transport.ulistener import UListener
from uprotocol.v1.ucode_pb2 import UCode
from uprotocol.v1.umessage_pb2 import UMessage
from uprotocol.v1.uri_pb2 import UUri
from uprotocol.v1.ustatus_pb2 import UStatus

from tests.testsupport.broker import fake_broker  # noqa: F401, F811
from up_client_mqtt5_python.mqtt5_utransport import MQTT5UTransport

logging.basicConfig(format="%(levelname)s| %(filename)s:%(lineno)s %(message)s")
logger = logging.getLogger("File:Line# Debugger")
logger.setLevel(logging.DEBUG)


def build_source():
    return UUri(authority_name="vcu.matthew.com", ue_id=1234, ue_version_major=1, resource_id=0x8000)


def build_sink():
    return UUri(authority_name="vcu.matthew.com", ue_id=1234, ue_version_major=1, resource_id=0)


def build_format_protobuf_upayload():
    return UPayload.pack(Timestamp(seconds=1000, nanos=1000))


def build_format_protobuf_any_upayload():
    return UPayload.pack_to_any(Timestamp(seconds=1000, nanos=1000))


def build_format_protobuf_json_upayload():
    json_data = {"key1": "value1", "key2": "value2"}
    return UPayload(value=json.dumps(json_data).encode("utf-8"), format=3)


def build_umessage(payload, source=build_source()):
    return UMessageBuilder.publish(source=source).build_from_upayload(payload)


class MQTT5UListener(UListener):
    def __init__(self) -> None:
        pass

    def on_receive(self, umsg: UMessage) -> None:
        """
        Method called to handle/process events.<br><br>
        Sends UMessage data directly to Test Manager
        @param topic: Topic the underlying source of the message.
        @param payload: Payload of the message.
        @param attributes: Transportation attributes.
        @return Returns an Ack every time a message is received and processed.
        """
        print(umsg)

        return UStatus(code=UCode.OK, message="all good")


class TestMQTT5UTransportSend:
    def test_utransport_send_valid_format_protobuf(self, fake_broker):  # noqa: F811
        transport = MQTT5UTransport(
            client_id="test_client",
            host_name="localhost",
            port=fake_broker.port,
            cloud_device=True,
        )
        transport.connect()
        umsg: UMessage = build_umessage(build_format_protobuf_upayload())
        status = transport.send(umsg)
        assert status.code == UCode.OK

    def test_utransport_send_valid_format_protobuf_any(self, fake_broker):  # noqa: F811
        transport = MQTT5UTransport(
            client_id="test_client",
            host_name="localhost",
            port=fake_broker.port,
            cloud_device=True,
        )
        transport.connect()
        umsg: UMessage = build_umessage(build_format_protobuf_any_upayload())
        status = transport.send(umsg)
        assert status.code == UCode.OK

    def test_utransport_send_valid_format_json(self, fake_broker):  # noqa: F811
        transport = MQTT5UTransport(
            client_id="test_client",
            host_name="localhost",
            port=fake_broker.port,
            cloud_device=True,
        )
        transport.connect()
        umsg: UMessage = build_umessage(build_format_protobuf_json_upayload())
        status = transport.send(umsg)
        assert status.code == UCode.OK

    def test_utransport_register_listener_valid(self, fake_broker):  # noqa: F811
        transport = MQTT5UTransport(
            client_id="test_client",
            host_name="localhost",
            port=fake_broker.port,
            cloud_device=True,
        )
        transport.connect()
        topic: UUri = build_source()
        status = transport.register_listener(topic, MQTT5UListener())
        assert status.code == UCode.OK
