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
import paho.mqtt.client as mqtt
import ssl
import threading
import socket

from uprotocol.transport.utransport import UTransport
from uprotocol.proto.ustatus_pb2 import UStatus, UCode
from uprotocol.proto.umessage_pb2 import UMessage
from uprotocol.proto.uattributes_pb2 import UMessageType
from uprotocol.proto.uri_pb2 import UUri
from uprotocol.proto.uuid_pb2 import UUID
from uprotocol.proto.upayload_pb2 import UPayload
from uprotocol.proto.uattributes_pb2 import UAttributes, UPriority
from uprotocol.transport.ulistener import UListener
from uprotocol.uri.validator.urivalidator import UriValidator

logging.basicConfig(
    format='%(levelname)s| %(filename)s:%(lineno)s %(message)s')
logger = logging.getLogger('File:Line# Debugger')
logger.setLevel(logging.DEBUG)


class MQTT5UTransport(UTransport):

    def __init__(self, client_id: str, host_name: str, port: int,
                 cloud_device: bool) -> None:
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

        self._connected_signal = threading.Event()

        self.topic_to_listener: Dict[bytes, List[UListener]] = {}
        self.reqid_to_future: Dict[bytes, Future] = {}

        self._mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                                        client_id=client_id,
                                        protocol=mqtt.MQTTv5)

        self._mqtt_client.enable_logger()

    def create_tls_context(self, certificate_filename: str = None,
                           key_filename: str = None,
                           key_pass_phrase: str = None,
                           ssl_method=ssl.PROTOCOL_TLSv1_2,
                           verify_mode=ssl.CERT_NONE,
                           check_hostname=False) -> None:
        self.context = ssl.SSLContext(protocol=ssl_method)
        self.context.verify_mode = verify_mode
        self.context.check_hostname = check_hostname
        if certificate_filename is not None:
            self.context.load_cert_chain(
                certificate_filename, key_filename, key_pass_phrase)
        self._mqtt_client.tls_set_context(self.context)

    def connect(self):
        self._mqtt_client.on_message = self._listen
        logger.info(f"{self.__class__.__name__} Connecting to MQTT Broker")
        self._mqtt_client.connect(
            host=self.host_name, port=self.port,
            clean_start=False, keepalive=60)
        logger.info(f"{self.__class__.__name__} Connected to MQTT Broker")
        self._mqtt_client.loop_start()
        logger.info(f"{self.__class__.__name__} started MQTT Loop")

    def _listen(self, client, userdata, msg):
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
            if user_property[0] == "upriority":
                attributes.priority = UPriority.Value(user_property[1])
            elif user_property[0] == "type":
                attributes.type = UMessageType.Value(user_property[1])
            elif user_property[0] == "reqId":
                attributes.reqid = UUID(bytes=user_property[1])

            # TODO: Implement Sink UserProperty with the Short Form URI
            # elif user_property[0] == "sink":
            #     micro_uri_serializer = MicroUriSerializer()
            #     attributes.sink.CopyFrom(
            #           micro_uri_serializer.deserialize(user_property[1]))
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

    def _handle_response_message(self, topic: str, umsg: UMessage):
        request_id: UUID = umsg.attributes.reqid
        request_id_b: bytes = request_id.SerializeToString()

        if request_id_b in self.reqid_to_future:
            respose_future: Future = self.reqid_to_future[request_id_b]
            respose_future.set_result(umsg)

            del self.reqid_to_future[request_id_b]

    def _handle_publish_message(self, topic: str, umsg: UMessage):
        if topic in self.topic_to_listener:
            logger.info(f"{self.__class__.__name__} Handle Topic")

            for listener in self.topic_to_listener[topic]:
                listener.on_receive(umsg)
        else:
            logger.info(
                f"{self.__class__.__name__} {topic} not found in Listener Map")

    def mqtt_topic_builder(self, topic: UUri, msg_type: str) -> str:
        """
        Builds MQTT topic based on whether the topic authority is
        local or remote.
        @param topic: UUri with which MQTT topics are built
        @param msg_type: Whether the topic is for sending or
        registering a listener
        @return: returns MQTT Topic
        """
        entity_id = str(topic.entity.id)
        version_major = str(topic.entity.version_major)
        resource_id = str(topic.resource.id)

        if UriValidator.is_local(topic.authority):
            return f"upl/{entity_id}/{version_major}/{resource_id}"

        try:
            authority_number = socket.inet_ntop(
                socket.AF_INET, topic.authority.ip)
        except ValueError:
            raise ValueError("Must provide an IP address for the authority.")

        if self.cloud_device:
            topic_type = "c2d" if msg_type == "send" else "d2c"
        else:
            topic_type = "d2c" if msg_type == "send" else "c2d"

        return (f"{topic_type}/{authority_number}/"
                f"{entity_id}/{version_major}/{resource_id}")

    def send(self, umsg: UMessage) -> UStatus:
        """
        Transmits UPayload to the topic using the attributes defined in
        UTransportAttributes.
        @param umsg: UMessage to be sent to MQTT
        @return:Returns OKSTATUS if the payload has been successfully
        sent (ACK'ed), otherwise it returns FAILSTATUS
        with the appropriate failure.
        """

        payload_proto: UPayload = umsg.payload
        uattributes_proto: UAttributes = umsg.attributes

        publish_properties = mqtt.Properties(mqtt.PacketTypes.PUBLISH)
        publish_properties.UserProperty = []
        try:
            publish_properties.UserProperty.append(
                ("upriority", UPriority.Name(uattributes_proto.priority)))
        except ValueError:
            raise ValueError("Priority not supported.")
        message_type: UMessageType = uattributes_proto.type
        publish_properties.UserProperty.append(
            ("type", UMessageType.Name(message_type)))
        if message_type == UMessageType.UMESSAGE_TYPE_RESPONSE:
            publish_properties.UserProperty.append(
                ("reqId", uattributes_proto.reqid))
        # TODO: Implement Sink UserProperty with the Short Form URI
        # if uattributes_proto.HasField("sink"):
        #     micro_uri_serializer = MicroUriSerializer()
        #     publish_properties.UserProperty.append(
        #         ("sink", micro_uri_serializer.serialize(
        #             uattributes_proto.sink)))

        # if payload.format in [0, 1]:
        #     # 0: UPAYLOAD_FORMAT_UNSPECIFIED,
        #     # 1: UPAYLOAD_FORMAT_PROTOBUF_WRAPPED_IN_ANY
        #     payload_any: Any = payload_proto.data
        #     payload = payload_any.SerializeToString()
        # elif payload.format == 2:
        #     # 2: UPAYLOAD_FORMAT_PROTOBUF
        #     payload = payload_proto.data.SerializeToString()
        # elif payload.format == 6:
        #     # 6: UPAYLOAD_FORMAT_RAW
        #     payload = payload.data
        # else:
        #     # 3: UPAYLOAD_FORMAT_JSON 4: UPAYLOAD_FORMAT_SOMEIP
        #     # 5: UPAYLOAD_FORMAT_SOMEIP_TLV
        #     # 7: UPAYLOAD_FORMAT_TEXT
        #     payload = base64.b64encode(payload_proto.data)

        payload: bytes = payload_proto.SerializeToString()
        self._mqtt_client.publish(
            topic=self.mqtt_topic_builder(
                topic=umsg.attributes.source, msg_type="send"),
            payload=payload, qos=1, properties=publish_properties)

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

        mqtt_topic = self.mqtt_topic_builder(topic=topic, msg_type="register")
        self.topic_to_listener.setdefault(
            mqtt_topic, []).append(listener)

        self._mqtt_client.subscribe(
            topic=mqtt_topic, qos=1)

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
        mqtt_topic = self.mqtt_topic_builder(topic=topic, msg_type="register")

        if mqtt_topic in self.topic_to_listener:
            if len(self.topic_to_listener[mqtt_topic]) > 1:
                self.topic_to_listener[mqtt_topic].remove(listener)
            else:
                del self.topic_to_listener[mqtt_topic]

        self._mqtt_client.unsubscribe(
            topic=mqtt_topic)

        return UStatus(code=UCode.OK, message="OK")
