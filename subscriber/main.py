"""Subscriber skeleton for RabbitMQ topic-exchange architecture.

Each subsystem owns its queue and subscription keys, and can subscribe/unsubscribe
without changing publisher code.
"""

from __future__ import annotations

import json
from typing import Any, Dict, Iterable

import pika


class SubsystemSubscriber:
    """A subsystem consumer with ephemeral queue semantics."""

    def __init__(
        self,
        amqp_url: str,
        subsystem_id: str,
        bindings: Iterable[str],
        exchange: str = "events.topic",
    ) -> None:
        self.amqp_url = amqp_url
        self.subsystem_id = subsystem_id
        self.bindings = list(bindings)
        self.exchange = exchange

    def _process_business(self, payload: Dict[str, Any]) -> None:
        """Business placeholder: fetch from Mongo by file_id and analyze."""
        file_id = payload.get("data", {}).get("file_id")
        event_type = payload.get("event_type")
        print(f"[{self.subsystem_id}] event={event_type} file_id={file_id} -> start analysis")

    def start(self) -> None:
        params = pika.URLParameters(self.amqp_url)
        connection = pika.BlockingConnection(params)
        channel = connection.channel()

        channel.exchange_declare(
            exchange=self.exchange,
            exchange_type="topic",
            durable=True,
            auto_delete=False,
        )

        result = channel.queue_declare(
            queue=f"{self.subsystem_id}.ephemeral",
            durable=False,
            exclusive=True,
            auto_delete=True,
            arguments={
                "x-message-ttl": 60_000,
                "x-expires": 300_000,
                "x-max-length": 1_000,
                "overflow": "drop-head",
            },
        )
        queue_name = result.method.queue

        for binding in self.bindings:
            channel.queue_bind(exchange=self.exchange, queue=queue_name, routing_key=binding)

        def on_message(ch: pika.adapters.blocking_connection.BlockingChannel, method, properties, body: bytes) -> None:
            payload = json.loads(body.decode("utf-8"))
            self._process_business(payload)
            # Best-effort notification stream: quick ack after local handling starts.
            ch.basic_ack(delivery_tag=method.delivery_tag)

        channel.basic_qos(prefetch_count=20)
        channel.basic_consume(queue=queue_name, on_message_callback=on_message, auto_ack=False)

        print(f"[{self.subsystem_id}] subscribed keys={self.bindings}, waiting for events...")
        channel.start_consuming()


if __name__ == "__main__":
    SubsystemSubscriber(
        amqp_url="amqp://guest:guest@localhost:5672/%2F",
        subsystem_id="risk-analyzer",
        bindings=["risk.file.ready.v1", "risk.preprocess.done.v1"],
    ).start()
