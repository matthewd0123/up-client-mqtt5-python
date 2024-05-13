import pytest
import logging
import socket
import json

from up_client_mqtt5_python.mqtt5_utransport import MQTT5UTransport
from tests.testsupport.broker import fake_broker  # noqa: F401, F811

from google.protobuf.timestamp_pb2 import Timestamp

from uprotocol.proto.umessage_pb2 import UMessage
from uprotocol.proto.uattributes_pb2 import UPriority
from uprotocol.proto.uri_pb2 import UUri, UAuthority, UEntity, UResource
from uprotocol.proto.ustatus_pb2 import UStatus, UCode
from uprotocol.proto.upayload_pb2 import UPayload
from uprotocol.transport.builder.uattributesbuilder import UAttributesBuilder
from uprotocol.transport.builder.upayloadbuilder import UPayloadBuilder
from uprotocol.uri.factory.uresource_builder import UResourceBuilder
from uprotocol.transport.ulistener import UListener

logging.basicConfig(
    format="%(levelname)s| %(filename)s:%(lineno)s %(message)s"
)
logger = logging.getLogger("File:Line# Debugger")
logger.setLevel(logging.DEBUG)


def build_source():
    return UUri(
        authority=UAuthority(
            name="vcu.someVin.veh.ultifi.gm.com",
            ip=bytes(socket.inet_pton(socket.AF_INET, "10.0.3.3")),
        ),
        entity=UEntity(name="petapp.ultifi.gm.com", version_major=1, id=1234),
        resource=UResourceBuilder.for_rpc_request(None),
    )


def build_source_no_authority():
    return UUri(
        entity=UEntity(name="petapp.ultifi.gm.com", version_major=1, id=1234),
        resource=UResourceBuilder.for_rpc_request(None),
    )


def build_source_no_entity():
    return UUri(
        authority=UAuthority(
            name="vcu.someVin.veh.ultifi.gm.com",
            ip=bytes(socket.inet_pton(socket.AF_INET, "10.0.3.3")),
        ),
        resource=UResourceBuilder.for_rpc_request(None),
    )


def build_source_no_resource():
    return UUri(
        authority=UAuthority(
            name="vcu.someVin.veh.ultifi.gm.com",
            ip=bytes(socket.inet_pton(socket.AF_INET, "10.0.3.3")),
        ),
        entity=UEntity(name="petapp.ultifi.gm.com", version_major=1, id=1234),
    )


def build_sink():
    return UUri(
        authority=UAuthority(
            name="vcu.someVin.veh.ultifi.gm.com",
            ip=bytes(socket.inet_pton(socket.AF_INET, "10.0.3.3")),
        ),
        entity=UEntity(name="petapp.ultifi.gm.com", version_major=1, id=1234),
        resource=UResourceBuilder.for_rpc_response(),
    )


def build_format_protobuf_upayload():
    return UPayloadBuilder.pack(Timestamp(seconds=1000, nanos=1000))


def build_format_protobuf_any_upayload():
    return UPayloadBuilder.pack_to_any(Timestamp(seconds=1000, nanos=1000))


def build_format_protobuf_json_upayload():
    json_data = {"key1": "value1", "key2": "value2"}
    return UPayload(value=json.dumps(json_data).encode("utf-8"), format=3)


def build_publish_uattributes(source):
    return (
        UAttributesBuilder.publish(source, UPriority.UPRIORITY_CS1)
        .withSink(build_sink())
        .build()
    )


def build_umessage(payload, source=build_source()):
    return UMessage(
        attributes=build_publish_uattributes(source), payload=payload
    )


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

    def test_utransport_send_no_authority(self, fake_broker):  # noqa: F811
        transport = MQTT5UTransport(
            client_id="test_client",
            host_name="localhost",
            port=fake_broker.port,
            cloud_device=True,
        )
        transport.connect()
        umsg: UMessage = build_umessage(
            build_format_protobuf_upayload(), build_source_no_authority()
        )
        status = transport.send(umsg)
        assert status.code == UCode.OK

    def test_utransport_send_no_entity(self, fake_broker):  # noqa: F811
        transport = MQTT5UTransport(
            client_id="test_client",
            host_name="localhost",
            port=fake_broker.port,
            cloud_device=True,
        )
        transport.connect()
        umsg: UMessage = build_umessage(
            build_format_protobuf_upayload(), build_source_no_entity()
        )
        assert umsg.attributes.source.entity == UEntity()
        with pytest.raises(
            ValueError,
            match="Entity is required in source when building topic",
        ):
            transport.send(umsg)

    def test_utransport_send_no_resource(self, fake_broker):  # noqa: F811
        transport = MQTT5UTransport(
            client_id="test_client",
            host_name="localhost",
            port=fake_broker.port,
            cloud_device=True,
        )
        transport.connect()
        umsg: UMessage = build_umessage(
            build_format_protobuf_upayload(), build_source_no_resource()
        )
        assert umsg.attributes.source.resource == UResource()
        with pytest.raises(
            ValueError,
            match="Resource is required in source when building topic",
        ):
            transport.send(umsg)

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

    def test_utransport_register_listener_no_authority(self, fake_broker):  # noqa: F811
        transport = MQTT5UTransport(
            client_id="test_client",
            host_name="localhost",
            port=fake_broker.port,
            cloud_device=True,
        )
        transport.connect()
        topic: UUri = build_source_no_authority()
        status = transport.register_listener(topic, MQTT5UListener())
        assert status.code == UCode.OK

    def test_utransport_register_listener_no_entity(self, fake_broker):  # noqa: F811
        transport = MQTT5UTransport(
            client_id="test_client",
            host_name="localhost",
            port=fake_broker.port,
            cloud_device=True,
        )
        transport.connect()
        topic: UUri = build_source_no_entity()
        assert topic.entity == UEntity()
        with pytest.raises(
            ValueError,
            match="Entity is required in source when building topic",
        ):
            transport.register_listener(topic, MQTT5UListener())

    def test_utransport_register_listener_no_resource(self, fake_broker):  # noqa: F811
        transport = MQTT5UTransport(
            client_id="test_client",
            host_name="localhost",
            port=fake_broker.port,
            cloud_device=True,
        )
        transport.connect()
        topic: UUri = build_source_no_resource()
        assert topic.resource == UResource()
        with pytest.raises(
            ValueError,
            match="Resource is required in source when building topic",
        ):
            transport.register_listener(topic, MQTT5UListener())
