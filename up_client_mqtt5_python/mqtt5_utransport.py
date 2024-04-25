# -------------------------------------------------------------------------
#
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
#
# -------------------------------------------------------------------------

from typing import Dict, List
from concurrent.futures import Future
import logging
import ssl
import threading
import socket
import paho.mqtt.client as mqtt

from uprotocol.transport.utransport import UTransport
from uprotocol.proto.ustatus_pb2 import UStatus, UCode
from uprotocol.proto.umessage_pb2 import UMessage
from uprotocol.proto.uattributes_pb2 import UMessageType
from uprotocol.proto.uri_pb2 import UUri, UEntity, UResource
from uprotocol.proto.uuid_pb2 import UUID
from uprotocol.proto.upayload_pb2 import UPayload
from uprotocol.proto.uattributes_pb2 import UAttributes, UPriority
from uprotocol.transport.ulistener import UListener
from uprotocol.uri.serializer.shorturiserializer import ShortUriSerializer
from uprotocol.cloudevent.serialize.base64protobufserializer import (
    Base64ProtobufSerializer,
)

logging.basicConfig(
    format="%(levelname)s| %(filename)s:%(lineno)s %(message)s"
)
logger = logging.getLogger("File:Line# Debugger")
logger.setLevel(logging.DEBUG)


class MQTT5UTransport(UTransport):
    """
    MQTTv5 Transport for UProtocol
    """

    def __init__(
        self, client_id: str, host_name: str, port: int, cloud_device: bool
    ) -> None:
        """
        Creates a UEntity with an MQTTv5 Connection, as well as tracking a
        list of registered listeners.
        @param client_id: ID of the MQTT Client
        @param host_name: Address of the MQTT Broker
        @param port: Port of the MQTT Broker
        @param cloud_device: Whether or not your device lives in the cloud.
        """

        self.host_name = host_name
        self.port = port
        self.cloud_device = cloud_device
        self.context = None

        self._connected_signal = threading.Event()

        self.topic_to_listener: Dict[bytes, List[UListener]] = {}
        self.reqid_to_future: Dict[bytes, Future] = {}

        self._mqtt_client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id,
            protocol=mqtt.MQTTv5,
        )

        self._mqtt_client.enable_logger()

    def create_tls_context(
        self,
        certificate_filename: str = None,
        key_filename: str = None,
        key_pass_phrase: str = None,
        ssl_method=ssl.PROTOCOL_TLSv1_2,
        verify_mode=ssl.CERT_NONE,
        check_hostname=False,
    ) -> None:
        """
        Creates a TLS Context for the MQTT Broker.
        @param certificate_filename: Filename of the certificate
        @param key_filename: Filename of the key
        @param key_pass_phrase: Passphrase for the key
        @param ssl_method: SSL Method
        @param verify_mode: Verification Mode
        @param check_hostname: Whether or not to check the hostname
        @return: None
        """

        self.context = ssl.SSLContext(protocol=ssl_method)
        self.context.verify_mode = verify_mode
        self.context.check_hostname = check_hostname
        if certificate_filename is not None:
            self.context.load_cert_chain(
                certificate_filename, key_filename, key_pass_phrase
            )
        self._mqtt_client.tls_set_context(self.context)

    def connect(self):
        """
        Connects to the MQTT Broker.
        @return: None
        """
        self._mqtt_client.on_message = self._listen
        logger.info("%s Connecting to MQTT Broker", self.__class__.__name__)
        self._mqtt_client.connect(
            host=self.host_name,
            port=self.port,
            clean_start=False,
            keepalive=60,
        )
        logger.info("%s Connected to MQTT Broker", self.__class__.__name__)
        self._mqtt_client.loop_start()
        logger.info("%s started MQTT Loop", self.__class__.__name__)

    def _listen(self, msg):
        """
        Listens for and processes messages from MQTT Broker.
        @param client:
        @param userdata:
        @param msg:
        @return: None
        """

        payload_data: UPayload = UPayload()
        payload_data.ParseFromString(msg.payload)
        umsg: UMessage = UMessage()
        umsg.payload.CopyFrom(payload_data)

        publish_properties = msg.properties

        attributes: UAttributes = UAttributes()
        for user_property in publish_properties.UserProperty:
            if user_property[0] == "1":
                id_proto: UUID = UUID()
                id_proto.ParseFromString(
                    Base64ProtobufSerializer().serialize(user_property[1])
                )
                attributes.id.CopyFrom(id_proto)
            elif user_property[0] == "2":
                attributes.type = UMessageType.Value(user_property[1])
            elif user_property[0] == "3":
                attributes.source.CopyFrom(
                    ShortUriSerializer().deserialize(user_property[1])
                )
            elif user_property[0] == "4":
                attributes.sink.CopyFrom(
                    ShortUriSerializer().deserialize(user_property[1])
                )
            elif user_property[0] == "5":
                attributes.priority = UPriority.Value(user_property[1])
            elif user_property[0] == "6":
                attributes.ttl = int(user_property[1])
            elif user_property[0] == "7":
                attributes.permission_level = int(user_property[1])
            elif user_property[0] == "8":
                attributes.commstatus = UCode.Value(user_property[1])
            elif user_property[0] == "9":
                reqid_proto: UUID = UUID()
                reqid_proto.ParseFromString(
                    Base64ProtobufSerializer().serialize(user_property[1])
                )
                attributes.reqid.CopyFrom(reqid_proto)
            elif user_property[0] == "10":
                attributes.token = user_property[1]
            elif user_property[0] == "11":
                attributes.traceparent = user_property[1]

        umsg.attributes.CopyFrom(attributes)

        message_type_handlers = {
            UMessageType.UMESSAGE_TYPE_PUBLISH: self._handle_publish_message,
            UMessageType.UMESSAGE_TYPE_REQUEST: self._handle_publish_message,
            UMessageType.UMESSAGE_TYPE_RESPONSE: self._handle_response_message,
        }

        handler = message_type_handlers.get(attributes.type)
        if handler:
            handler(msg.topic, umsg)
        else:
            raise ValueError("Unsupported message type: " + attributes.type)

    def _handle_response_message(self, umsg: UMessage):
        request_id: UUID = umsg.attributes.reqid
        request_id_b: bytes = request_id.SerializeToString()

        if request_id_b in self.reqid_to_future:
            respose_future: Future = self.reqid_to_future[request_id_b]
            respose_future.set_result(umsg)

            del self.reqid_to_future[request_id_b]

    def _handle_publish_message(self, topic: str, umsg: UMessage):
        if topic in self.topic_to_listener:
            logger.info(
                "%s Handle Topic",
                self.__class__.__name__
            )

            for listener in self.topic_to_listener[topic]:
                listener.on_receive(umsg)
        else:
            logger.info(
                "%s %s not found in Listener Map",
                self.__class__.__name__,
                topic
            )

    def mqtt_topic_builder(self, topic: UUri) -> str:
        """
        Builds MQTT topic based on whether the topic authority is
        local or remote.
        @param topic: UUri with which MQTT topics are built
        @param msg_type: Whether the topic is for sending or
        registering a listener
        @return: returns MQTT Topic
        """
        if topic.entity in [None, UEntity()]:
            raise ValueError(
                "Entity is required in source when building topic"
            )
        entity_id = str(topic.entity.id)
        version_major = str(topic.entity.version_major)
        if topic.resource in [None, UResource()]:
            raise ValueError(
                "Resource is required in source when building topic"
            )
        resource_id = str(topic.resource.id)

        if topic.authority.HasField("ip"):
            try:
                authority_number = socket.inet_ntop(
                    socket.AF_INET, topic.authority.ip
                )
            except ValueError as e:
                raise ValueError(e) from e
        elif topic.authority.HasField("id"):
            authority_number = topic.authority.id.decode("utf-8")
        else:
            authority_number = self.host_name

        return f"{authority_number}/{entity_id}/{version_major}/{resource_id}"

    def send(self, message: UMessage) -> UStatus:
        """
        Transmits UPayload to the topic using the attributes defined in
        UTransportAttributes.
        @param umsg: UMessage to be sent to MQTT
        @return:Returns OKSTATUS if the payload has been successfully
        sent (ACK'ed), otherwise it returns FAILSTATUS
        with the appropriate failure.
        """

        payload_proto: UPayload = message.payload
        uattributes_proto: UAttributes = message.attributes

        publish_properties = mqtt.Properties(mqtt.PacketTypes.PUBLISH)
        publish_properties.UserProperty = []
        try:
            if uattributes_proto.HasField("id"):
                publish_properties.UserProperty.append(
                    (
                        "1",
                        Base64ProtobufSerializer().deserialize(
                            uattributes_proto.id.SerializeToString()
                        ),
                    )
                )
            publish_properties.UserProperty.append(
                ("2", UMessageType.Name(uattributes_proto.type))
            )
            if uattributes_proto.HasField("source"):
                publish_properties.UserProperty.append(
                    (
                        "3",
                        ShortUriSerializer().serialize(
                            uattributes_proto.source
                        ),
                    )
                )
            if uattributes_proto.HasField("sink"):
                publish_properties.UserProperty.append(
                    (
                        "4",
                        ShortUriSerializer().serialize(uattributes_proto.sink),
                    )
                )
            publish_properties.UserProperty.append(
                ("5", UPriority.Name(uattributes_proto.priority))
            )
            if uattributes_proto.HasField("ttl"):
                publish_properties.UserProperty.append(
                    ("6", str(uattributes_proto.ttl))
                )
            if uattributes_proto.HasField("permission_level"):
                publish_properties.UserProperty.append(
                    ("7", str(uattributes_proto.permission_level))
                )
            if uattributes_proto.HasField("commstatus"):
                publish_properties.UserProperty.append(
                    ("8", UCode.Name(uattributes_proto.commstatus))
                )
            if uattributes_proto.type == UMessageType.UMESSAGE_TYPE_RESPONSE:
                publish_properties.UserProperty.append(
                    (
                        "9",
                        Base64ProtobufSerializer().deserialize(
                            uattributes_proto.reqid.SerializeToString()
                        ),
                    )
                )
            if uattributes_proto.HasField("token"):
                publish_properties.UserProperty.append(
                    ("10", uattributes_proto.token)
                )
            if uattributes_proto.HasField("traceparent"):
                publish_properties.UserProperty.append(
                    ("11", uattributes_proto.traceparent)
                )
        except ValueError as e:
            raise ValueError(e) from e

        payload: bytes = payload_proto.SerializeToString()
        self._mqtt_client.publish(
            topic=self.mqtt_topic_builder(
                topic=message.attributes.source
            ),
            payload=payload,
            qos=1,
            properties=publish_properties,
        )

        return UStatus(code=UCode.OK, message="OK")

    def register_listener(self, topic: UUri, listener: UListener) -> UStatus:
        """
        Register listener to be called when UPayload is received for the
        specific topic.
        @param topic:Resolved UUri for where the message arrived via
        the underlying transport technology.
        @param listener:The method to execute to process the date for the
        topic.
        @return:Returns OKSTATUS if the listener is registered
        correctly, otherwise it returns FAILSTATUS with the
        appropriate failure.
        """

        mqtt_topic = self.mqtt_topic_builder(topic=topic)
        self.topic_to_listener.setdefault(mqtt_topic, []).append(listener)

        self._mqtt_client.subscribe(topic=mqtt_topic, qos=1)

        self._mqtt_client.loop_start()

        return UStatus(code=UCode.OK, message="OK")

    def unregister_listener(self, topic: UUri, listener: UListener) -> UStatus:
        """
        Register listener to be called when UPayload is received for the
        specific topic.
        @param topic:Resolved UUri for where the message arrived via
        the underlying transport technology.
        @param listener:The method to execute to process the date for the
        topic.
        @return:Returns OKSTATUS if the listener is registered
        correctly, otherwise it returns FAILSTATUS with the
        appropriate failure.
        """
        mqtt_topic = self.mqtt_topic_builder(topic=topic)

        if mqtt_topic in self.topic_to_listener:
            if len(self.topic_to_listener[mqtt_topic]) > 1:
                self.topic_to_listener[mqtt_topic].remove(listener)
            else:
                del self.topic_to_listener[mqtt_topic]

        self._mqtt_client.unsubscribe(topic=mqtt_topic)

        return UStatus(code=UCode.OK, message="OK")
