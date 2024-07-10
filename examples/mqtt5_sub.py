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

from uprotocol.transport.ulistener import UListener
from uprotocol.v1.ucode_pb2 import UCode
from uprotocol.v1.umessage_pb2 import UMessage
from uprotocol.v1.uri_pb2 import UUri
from uprotocol.v1.ustatus_pb2 import UStatus

from up_client_mqtt5_python.mqtt5_utransport import MQTT5UTransport

logging.basicConfig(format='%(levelname)s| %(filename)s:%(lineno)s %(message)s')
logger = logging.getLogger('File:Line# Debugger')
logger.setLevel(logging.DEBUG)


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


def build_source():
    return UUri(authority_name="vcu.matthew.com", ue_id=1234, ue_version_major=1, resource_id=0x8000)


if __name__ == "__main__":
    mqtt5_subscriber = MQTT5UTransport(build_source(), "client_sub", "127.0.0.1", 1883, False)
    mqtt5_subscriber.connect()
    source: UUri = build_source()
    listener: MQTT5UListener = MQTT5UListener()
    mqtt5_subscriber.register_listener(source, listener)
    while True:
        time.sleep(10)
