"""Publisher demo invocation.

Run this file after RabbitMQ is available locally.
"""

from main import publish_file_ready


if __name__ == "__main__":
    publish_file_ready(
        amqp_url="amqp://guest:guest@localhost:5672/%2F",
        file_id="F-DEMO-0001",
        trace_id="trace-demo-pub",
        mongo_collection="preprocess_results",
        preprocess_version="v1.0.0",
    )
    print("publisher demo message sent")
