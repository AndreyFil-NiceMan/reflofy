"""Unit tests for KafkaDestination key_field payload selection (no real Kafka needed)."""

from unittest.mock import AsyncMock, patch

from reflowfy.destinations.kafka import kafka_destination


async def test_key_field_present_sends_only_that_field():
    dest = kafka_destination(
        bootstrap_servers="localhost:9092", topic="t", key_field="payload"
    )
    mock_producer = AsyncMock()
    with patch.object(dest, "_get_producer", return_value=mock_producer):
        await dest.send([{"payload": {"user_id": "42"}, "trace_id": "ignored"}])

    call = mock_producer.send_and_wait.call_args
    assert call.kwargs["value"] == b'{"user_id": "42"}'


async def test_no_key_field_configured_sends_whole_record():
    dest = kafka_destination(bootstrap_servers="localhost:9092", topic="t")
    mock_producer = AsyncMock()
    with patch.object(dest, "_get_producer", return_value=mock_producer):
        await dest.send([{"user_id": "42"}])

    call = mock_producer.send_and_wait.call_args
    assert call.kwargs["value"] == b'{"user_id": "42"}'


async def test_key_field_missing_on_record_sends_whole_record():
    dest = kafka_destination(
        bootstrap_servers="localhost:9092", topic="t", key_field="payload"
    )
    mock_producer = AsyncMock()
    with patch.object(dest, "_get_producer", return_value=mock_producer):
        await dest.send([{"name": "alice"}])

    call = mock_producer.send_and_wait.call_args
    assert call.kwargs["value"] == b'{"name": "alice"}'
