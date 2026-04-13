"""Publisher skeleton for RabbitMQ topic-exchange architecture.

This module focuses on publishing domain events after data is persisted to MongoDB.
It intentionally keeps message delivery as best-effort (transient), matching the
user's desired behavior: online subscribers receive; offline subscribers miss.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any, Dict

import pika


@dataclass
class EventEnvelope:
    """Standard event envelope used by all published messages."""

    event_id: str
    event_type: str
    occurred_at: str
    trace_id: str
    source: str
    data: Dict[str, Any]

    @classmethod
    def create(
        cls,
        event_type: str,
        trace_id: str,
        source: str,
        data: Dict[str, Any],
    ) -> "EventEnvelope":
        return cls(
            event_id=str(uuid.uuid4()),
            event_type=event_type,
            occurred_at=datetime.now(timezone.utc).isoformat(),
            trace_id=trace_id,
            source=source,
            data=data,
        )


class RabbitPublisher:
    """RabbitMQ publisher using topic exchange with transient messages."""

    def __init__(
        self,
        amqp_url: str,
        exchange: str = "events.topic",
    ) -> None:
        self.amqp_url = amqp_url
        self.exchange = exchange

    def publish(self, routing_key: str, envelope: EventEnvelope) -> None:
        params = pika.URLParameters(self.amqp_url)
        connection = pika.BlockingConnection(params)
        channel = connection.channel()

        # Idempotent declaration. Platform-owned exchange definition.
        channel.exchange_declare(
            exchange=self.exchange,
            exchange_type="topic",
            durable=True,
            auto_delete=False,
        )

        channel.basic_publish(
            exchange=self.exchange,
            routing_key=routing_key,
            body=json.dumps(asdict(envelope), ensure_ascii=False).encode("utf-8"),
            # delivery_mode=1 => transient/best-effort event
            properties=pika.BasicProperties(content_type="application/json", delivery_mode=1),
            mandatory=False,
        )
        connection.close()


def publish_file_ready(
    amqp_url: str,
    file_id: str,
    trace_id: str,
    mongo_collection: str,
    preprocess_version: str,
) -> None:
    """Reference publishing entrypoint used by the main system."""
    publisher = RabbitPublisher(amqp_url=amqp_url)
    event = EventEnvelope.create(
        event_type="risk.file.ready.v1",
        trace_id=trace_id,
        source="main-system",
        data={
            "file_id": file_id,
            "mongo_collection": mongo_collection,
            "preprocess_version": preprocess_version,
        },
    )
    publisher.publish(routing_key="risk.file.ready.v1", envelope=event)


if __name__ == "__main__":
    # Example only
    publish_file_ready(
        amqp_url="amqp://guest:guest@localhost:5672/%2F",
        file_id="F20260413001",
        trace_id="trace-123",
        mongo_collection="preprocess_results",
        preprocess_version="v1.0.0",
    )
