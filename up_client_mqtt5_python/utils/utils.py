"""
SPDX-FileCopyrightText: Copyright (c) 2024 Contributors to the
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

import paho.mqtt.client as mqtt
from uprotocol.communication.upayload import UPayload
from uprotocol.transport.builder.umessagebuilder import UMessageBuilder
from uprotocol.uri.serializer.uriserializer import UriSerializer
from uprotocol.uuid.serializer.uuidserializer import UuidSerializer
from uprotocol.v1.uattributes_pb2 import UAttributes, UMessageType, UPriority
from uprotocol.v1.ucode_pb2 import UCode
from uprotocol.v1.umessage_pb2 import UMessage


def build_message_from_mqtt_message_and_attributes(msg: mqtt.MQTTMessage, attributes: UAttributes) -> UMessage:
    """
    Build a message from a MQTT message and UAttributes
    :param msg: MQTT message
    :param attributes: UAttributes attributes
    :return: UMessage message
    """
    payload_data: UPayload = UPayload(msg.payload, attributes.payload_format)
    if attributes.type == UMessageType.UMESSAGE_TYPE_RESPONSE:
        return UMessageBuilder.response(attributes.source, attributes.sink, attributes.reqid).build_from_upayload(
            payload_data
        )
    elif attributes.type == UMessageType.UMESSAGE_TYPE_PUBLISH:
        return UMessageBuilder.publish(attributes.source).build_from_upayload(payload_data)
    elif attributes.type == UMessageType.UMESSAGE_TYPE_REQUEST:
        return UMessageBuilder.request(attributes.source, attributes.sink, attributes.ttl).build_from_upayload(
            payload_data
        )
    elif attributes.type == UMessageType.UMESSAGE_TYPE_NOTIFICATION:
        return UMessageBuilder.notification(attributes.source, attributes.sink).build_from_upayload(payload_data)


def build_attributes_from_mqtt_properties(publish_properties) -> UAttributes:
    """
    Build UAttributes from MQTT properties
    :param properties: MQTT properties
    :return: UAttributes attributes
    """
    attributes: UAttributes = UAttributes()
    for user_property in publish_properties.UserProperty:
        if user_property[0] == "1":
            attributes.id.CopyFrom(UuidSerializer.deserialize(user_property[1]))
        elif user_property[0] == "2":
            attributes.type = UMessageType.Value(user_property[1])
        elif user_property[0] == "3":
            attributes.source.CopyFrom(UriSerializer.deserialize(user_property[1]))
        elif user_property[0] == "4":
            attributes.sink.CopyFrom(UriSerializer.deserialize(user_property[1]))
        elif user_property[0] == "5":
            attributes.priority = UPriority.Value(user_property[1])
        elif user_property[0] == "6":
            attributes.ttl = int(user_property[1])
        elif user_property[0] == "7":
            attributes.permission_level = int(user_property[1])
        elif user_property[0] == "8":
            attributes.commstatus = UCode.Value(user_property[1])
        elif user_property[0] == "9":
            attributes.reqid.CopyFrom(UuidSerializer.deserialize(user_property[1]))
        elif user_property[0] == "10":
            attributes.token = user_property[1]
        elif user_property[0] == "11":
            attributes.traceparent = user_property[1]
        elif user_property[0] == "12":
            attributes.payload_format = user_property[1]
    return attributes


def build_mqtt_properties_from_attributes(attributes: UAttributes):
    """
    Build MQTT properties from UAttributes
    :param attributes: UAttributes attributes
    :return: MQTT properties
    """
    publish_properties = mqtt.Properties(mqtt.PacketTypes.PUBLISH)
    publish_properties.UserProperty = []
    try:
        if attributes.HasField("id"):
            publish_properties.UserProperty.append(("1", UuidSerializer.serialize(attributes.id)))
        publish_properties.UserProperty.append(("2", UMessageType.Name(attributes.type)))
        if attributes.HasField("source"):
            publish_properties.UserProperty.append(("3", UriSerializer.serialize(attributes.source)))
        if attributes.HasField("sink"):
            publish_properties.UserProperty.append(
                (
                    "4",
                    UriSerializer.serialize(attributes.sink),
                )
            )
        publish_properties.UserProperty.append(("5", UPriority.Name(attributes.priority)))
        if attributes.HasField("ttl"):
            publish_properties.UserProperty.append(("6", str(attributes.ttl)))
        if attributes.HasField("permission_level"):
            publish_properties.UserProperty.append(("7", str(attributes.permission_level)))
        if attributes.HasField("commstatus"):
            publish_properties.UserProperty.append(("8", UCode.Name(attributes.commstatus)))
        if attributes.type == UMessageType.UMESSAGE_TYPE_RESPONSE:
            publish_properties.UserProperty.append(
                (
                    "9",
                    UuidSerializer.serialize(attributes.reqid),
                )
            )
        if attributes.HasField("token"):
            publish_properties.UserProperty.append(("10", attributes.token))
        if attributes.HasField("traceparent"):
            publish_properties.UserProperty.append(("11", attributes.traceparent))
    except ValueError as e:
        raise ValueError(e) from e

    return publish_properties
