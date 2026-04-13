"""Subscriber demo invocation.

Run this file while RabbitMQ is running; then run publisher/test.py to trigger events.
"""

from main import SubsystemSubscriber


if __name__ == "__main__":
    SubsystemSubscriber(
        amqp_url="amqp://guest:guest@localhost:5672/%2F",
        subsystem_id="risk-analyzer-demo",
        bindings=["risk.file.ready.v1"],
    ).start()
