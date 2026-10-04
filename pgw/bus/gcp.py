"""Google Cloud Pub/Sub implementation of the bus.

    export PUBSUB_EMULATOR_HOST=localhost:8085   # local emulator (docker-compose), or unset for real GCP
    bus = GcpPubSubBus(project="pgw-local")

Topics and subscriptions are created on first use when `create=True` (the emulator).
In GCP they are created by Terraform (deploy/terraform), which also attaches the
dead-letter topics and retry policies. Messages are JSON; attributes carry the
trace context.

The Pub/Sub client delivers messages on its own threads. Handlers here are
coroutines, so each message is handed to the event loop and acked or nacked
depending on whether the handler succeeded.
"""

from __future__ import annotations

import asyncio
import json

from .base import Handler, Message


class GcpPubSubBus:
    def __init__(self, project: str, create: bool = False, ack_timeout_s: float = 30.0, suffix: str = ""):
        from google.cloud import pubsub_v1  # pip install ".[gcp]"

        self.project = project
        self.create = create
        self.ack_timeout_s = ack_timeout_s
        self.suffix = suffix  # Terraform names resources per environment: vehicle-command-results-dev
        self.publisher = pubsub_v1.PublisherClient()
        self.subscriber = pubsub_v1.SubscriberClient()
        self._futures = []

    def _topic(self, topic: str) -> str:
        return self.publisher.topic_path(self.project, topic + self.suffix)

    def _ensure_topic(self, topic: str) -> str:
        path = self._topic(topic)
        if self.create:
            from google.api_core.exceptions import AlreadyExists

            try:
                self.publisher.create_topic(name=path)
            except AlreadyExists:
                pass
        return path

    async def publish(self, topic: str, data: dict, attributes: dict[str, str] | None = None) -> str:
        path = await asyncio.to_thread(self._ensure_topic, topic)
        fut = self.publisher.publish(path, json.dumps(data).encode(), **(attributes or {}))
        return await asyncio.wrap_future(fut)

    async def subscribe(self, topic: str, subscription: str, handler: Handler) -> None:
        topic_path = await asyncio.to_thread(self._ensure_topic, topic)
        sub_path = self.subscriber.subscription_path(self.project, subscription + self.suffix)
        if self.create:
            from google.api_core.exceptions import AlreadyExists

            try:
                await asyncio.to_thread(self.subscriber.create_subscription, name=sub_path, topic=topic_path)
            except AlreadyExists:
                pass
        loop = asyncio.get_running_loop()

        def callback(raw) -> None:  # runs on a Pub/Sub client thread
            msg = Message(raw.message_id, topic, json.loads(raw.data), dict(raw.attributes),
                          (raw.delivery_attempt or 1))
            fut = asyncio.run_coroutine_threadsafe(handler(msg), loop)
            try:
                fut.result(timeout=self.ack_timeout_s)
                raw.ack()
            except Exception:  # noqa: BLE001 - nack: Pub/Sub redelivers, then dead-letters per policy
                raw.nack()

        self._futures.append(self.subscriber.subscribe(sub_path, callback=callback))

    async def close(self) -> None:
        for f in self._futures:
            f.cancel()
        self._futures.clear()
        await asyncio.to_thread(self.subscriber.close)
