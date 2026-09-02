"""Unit tests for KafkaDestination key_field extraction (no real Kafka needed)."""

from unittest.mock import AsyncMock, patch

from reflowfy.destinations.kafka import kafka_destination


async def test_key_field_extracted_and_kept_in_value():
    dest = kafka_destination(
        bootstrap_servers="localhost:9092", topic="t", key_field="user_id"
    )
    mock_producer = AsyncMock()
    with patch.object(dest, "_get_producer", return_value=mock_producer):
        await dest.send([{"user_id": "42", "name": "alice"}])

    call = mock_producer.send_and_wait.call_args
    assert call.kwargs["key"] == b"42"
    assert b'"user_id": "42"' in call.kwargs["value"]


async def test_no_key_field_configured_sends_no_key():
    dest = kafka_destination(bootstrap_servers="localhost:9092", topic="t")
    mock_producer = AsyncMock()
    with patch.object(dest, "_get_producer", return_value=mock_producer):
        await dest.send([{"user_id": "42"}])

    assert mock_producer.send_and_wait.call_args.kwargs["key"] is None


async def test_key_field_missing_on_record_sends_no_key():
    dest = kafka_destination(
        bootstrap_servers="localhost:9092", topic="t", key_field="user_id"
    )
    mock_producer = AsyncMock()
    with patch.object(dest, "_get_producer", return_value=mock_producer):
        await dest.send([{"name": "alice"}])

    assert mock_producer.send_and_wait.call_args.kwargs["key"] is None
