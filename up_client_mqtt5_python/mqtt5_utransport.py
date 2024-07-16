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
import ssl
import threading
from concurrent.futures import Future
from typing import Dict, List

import paho.mqtt.client as mqtt
from uprotocol.transport.ulistener import UListener
from uprotocol.transport.utransport import UTransport
from uprotocol.v1.uattributes_pb2 import UAttributes, UMessageType
from uprotocol.v1.ucode_pb2 import UCode
from uprotocol.v1.umessage_pb2 import UMessage
from uprotocol.v1.uri_pb2 import UUri
from uprotocol.v1.ustatus_pb2 import UStatus
from uprotocol.v1.uuid_pb2 import UUID

from up_client_mqtt5_python.utils.utils import (
    build_attributes_from_mqtt_properties,
    build_message_from_mqtt_message_and_attributes,
    build_mqtt_properties_from_attributes,
    uuri_field_resolver,
)

logging.basicConfig(format="%(levelname)s| %(filename)s:%(lineno)s %(message)s")
logger = logging.getLogger("File:Line# Debugger")
logger.setLevel(logging.DEBUG)


class MQTT5UTransport(UTransport):
    """
    MQTTv5 Transport for UProtocol
    """

    def __init__(self, source: UUri, client_id: str, host_name: str, port: int, cloud_device: bool) -> None:
        """
        Creates a UEntity with an MQTTv5 Connection, as well as tracking a
        list of registered listeners.
        @param client_id: ID of the MQTT Client
        @param host_name: Address of the MQTT Broker
        @param port: Port of the MQTT Broker
        @param cloud_device: Whether or not your device lives in the cloud.
        """

        self.source = source
        self.host_name = host_name
        self.port = port
        self.cloud_device = cloud_device
        self.context = None

        self._connected_signal = threading.Event()

        self.topic_to_listener: Dict[str, List[UListener]] = {}
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
            self.context.load_cert_chain(certificate_filename, key_filename, key_pass_phrase)
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

    def _listen(self, client, userdata, msg):
        """
        Listens for and processes messages from MQTT Broker.
        @param client:
        @param userdata:
        @param msg:
        @return: None
        """
        logger.info(f"Received Message on MQTT: {msg}")

        attributes: UAttributes = build_attributes_from_mqtt_properties(msg.properties)
        umsg: UMessage = build_message_from_mqtt_message_and_attributes(msg, attributes)

        message_type_handlers = {
            UMessageType.UMESSAGE_TYPE_UNSPECIFIED: self._handle_unspecified_message,
            UMessageType.UMESSAGE_TYPE_PUBLISH: self._handle_publish_message,
            UMessageType.UMESSAGE_TYPE_REQUEST: self._handle_publish_message,
            UMessageType.UMESSAGE_TYPE_RESPONSE: self._handle_response_message,
        }

        handler = message_type_handlers.get(attributes.type)
        if handler:
            handler(msg.topic, umsg)
        else:
            raise ValueError("Unsupported message type: " + UMessageType.Name(attributes.type))

    def _handle_unspecified_message(self, topic: str, umsg: UMessage):
        logger.info("%s Unspecified Message Received", self.__class__.__name__)
        logger.info(f"Message Details: {umsg}")
        logger.info(f"Unspecified Message received on topic {topic}")

    def _handle_response_message(self, topic: str, umsg: UMessage):
        request_id: UUID = umsg.attributes.reqid
        request_id_b: bytes = request_id.SerializeToString()

        if request_id_b in self.reqid_to_future:
            respose_future: Future = self.reqid_to_future[request_id_b]
            respose_future.set_result(umsg)

            del self.reqid_to_future[request_id_b]

    def _handle_publish_message(self, topic: str, umsg: UMessage):
        if topic in self.topic_to_listener:
            logger.info("%s Handle Publish Message on Topic", self.__class__.__name__)

            for listener in self.topic_to_listener[topic]:
                listener.on_receive(umsg)
        else:
            logger.info("%s %s not found in Listener Map", self.__class__.__name__, topic)

    def mqtt_topic_builder(self, source: UUri, sink: UUri = None) -> str:
        """
        Builds MQTT topic based on whether the topic authority is
        local or remote.
        @param topic: UUri with which MQTT topics are built
        @param msg_type: Whether the topic is for sending or
        registering a listener
        @return: returns MQTT Topic
        """

        device = "c" if self.cloud_device else "d"
        if source != UUri():
            src_auth_name = source.authority_name if source != UUri() else "+"
            src_ue_id = uuri_field_resolver(source.ue_id, 0xFFFF, "ffff")
            src_ue_version_major = uuri_field_resolver(source.ue_version_major, 0xFF, "ff")
            src_resource_id = uuri_field_resolver(source.resource_id, 0xFFFF, "ffff")
        topic = device + "/" + src_auth_name + "/" + src_ue_id + "/" + src_ue_version_major + "/" + src_resource_id
        if sink is not None and sink != UUri():
            sink_auth_name = sink.authority_name
            sink_ue_id = uuri_field_resolver(sink.ue_id, 0xFFFF, "ffff")
            sink_ue_version_major = uuri_field_resolver(sink.ue_version_major, 0xFF, "ff")
            sink_resource_id = uuri_field_resolver(sink.resource_id, 0xFFFF, "ffff")
            topic += "/" + sink_auth_name + "/" + sink_ue_id + "/" + sink_ue_version_major + "/" + sink_resource_id
        return topic

    def send(self, message: UMessage) -> UStatus:
        """
        Transmits UPayload to the topic using the attributes defined in
        UTransportAttributes.
        @param umsg: UMessage to be sent to MQTT
        @return:Returns OKSTATUS if the payload has been successfully
        sent (ACK'ed), otherwise it returns FAILSTATUS
        with the appropriate failure.
        """

        payload: bytes = message.payload

        publish_properties = build_mqtt_properties_from_attributes(message.attributes)

        self._mqtt_client.publish(
            topic=self.mqtt_topic_builder(source=message.attributes.source, sink=message.attributes.sink),
            payload=payload,
            qos=1,
            properties=publish_properties,
        )

        return UStatus(code=UCode.OK, message="OK")

    def register_listener(self, source_filter: UUri, listener: UListener, sink_filter: UUri = None) -> UStatus:
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

        mqtt_topic = self.mqtt_topic_builder(source=source_filter, sink=sink_filter)
        logger.info("%s Registering Listener for Topic: %s", self.__class__.__name__, mqtt_topic)

        self.topic_to_listener.setdefault(mqtt_topic, []).append(listener)

        self._mqtt_client.subscribe(topic=mqtt_topic, qos=1)

        self._mqtt_client.loop_start()

        return UStatus(code=UCode.OK, message="OK")

    def unregister_listener(self, source_filter: UUri, listener: UListener, sink_filter: UUri) -> UStatus:
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
        mqtt_topic = self.mqtt_topic_builder(source=source_filter, sink=sink_filter)

        if mqtt_topic in self.topic_to_listener:
            if len(self.topic_to_listener[mqtt_topic]) > 1:
                self.topic_to_listener[mqtt_topic].remove(listener)
            else:
                del self.topic_to_listener[mqtt_topic]

        self._mqtt_client.unsubscribe(topic=mqtt_topic)

        return UStatus(code=UCode.OK, message="OK")

    def get_source(self) -> UUri:
        """
        Returns the source of the MQTT Transport.
        @return: UUri source
        """
        return self.source

    def close(self):
        """
        Closes the MQTT Connection.
        @return: None
        """
        self._mqtt_client.disconnect()
        self._mqtt_client.loop_stop()
